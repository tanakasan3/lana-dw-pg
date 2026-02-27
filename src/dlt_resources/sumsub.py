"""Sumsub applicants DLT resource with PostgreSQL support."""

from __future__ import annotations

import base64
import hashlib
import hmac
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dlt
from dlt.sources.helpers import requests
from requests.exceptions import RequestException
from sqlalchemy import create_engine, text

REQUEST_TIMEOUT = 60
SUMSUB_API_BASE = "https://api.sumsub.com"
SUMSUB_APPLICANTS_DLT_TABLE = "sumsub_applicants_dlt"


def _sumsub_send(
    session: requests.Session,
    method: str,
    url: str,
    key: str,
    secret: str,
    body: Optional[bytes] = None,
) -> requests.Response:
    """Prepare and send a signed Sumsub API request."""
    req = requests.Request(method, url, data=body)
    prepared = session.prepare_request(req)

    now_ts = int(time.time())
    method_upper = method.upper()
    path_url = prepared.path_url
    body_bytes = b"" if prepared.body is None else prepared.body
    if isinstance(body_bytes, str):
        body_bytes = body_bytes.encode("utf-8")

    data_to_sign = (
        str(now_ts).encode("utf-8")
        + method_upper.encode("utf-8")
        + path_url.encode("utf-8")
        + body_bytes
    )
    signature = hmac.new(secret.encode("utf-8"), data_to_sign, hashlib.sha256)

    prepared.headers["accept"] = "application/json"
    prepared.headers["X-App-Token"] = key
    prepared.headers["X-App-Access-Ts"] = str(now_ts)
    prepared.headers["X-App-Access-Sig"] = signature.hexdigest()

    return session.send(prepared, timeout=REQUEST_TIMEOUT)


def _get_applicant_data(
    session: requests.Session, external_user_id: str, key: str, secret: str
) -> requests.Response:
    url = f"{SUMSUB_API_BASE}/resources/applicants/-;externalUserId={external_user_id}/one"
    return _sumsub_send(session, "GET", url, key, secret)


def _get_document_metadata(
    session: requests.Session, applicant_id: str, key: str, secret: str
) -> Dict[str, Any]:
    url = f"{SUMSUB_API_BASE}/resources/applicants/{applicant_id}/metadata/resources"
    resp = _sumsub_send(session, "GET", url, key, secret)
    resp.raise_for_status()
    return resp.json()


def _download_document_image(
    session: requests.Session, inspection_id: str, image_id: str, key: str, secret: str
) -> Optional[str]:
    url = (
        f"{SUMSUB_API_BASE}/resources/inspections/{inspection_id}/resources/{image_id}"
    )
    resp = _sumsub_send(session, "GET", url, key, secret)
    if resp.status_code == 200:
        return base64.b64encode(resp.content).decode("utf-8")
    return None


def _get_customers_pg(
    connection_string: str, schema: str, since: datetime
) -> List[Tuple[str, datetime]]:
    """Return (customer_id, max_recorded_at) for inbox events on/after 'since', ordered by max_recorded_at."""
    engine = create_engine(connection_string)
    
    sql = text(f"""
        WITH customers AS (
            SELECT
                payload->>'externalUserId' AS customer_id,
                MAX(recorded_at) AS recorded_at
            FROM {schema}.inbox_events
            WHERE recorded_at > :since
              AND payload->>'externalUserId' IS NOT NULL
            GROUP BY customer_id
        )
        SELECT customer_id, recorded_at
        FROM customers
        ORDER BY recorded_at ASC
    """)
    
    with engine.connect() as conn:
        result = conn.execute(sql, {"since": since})
        return [(row.customer_id, row.recorded_at) for row in result]


@dlt.resource(
    name=SUMSUB_APPLICANTS_DLT_TABLE,
    write_disposition="append",
    primary_key=["customer_id", "recorded_at"],
)
def applicants(
    dest_connection_string: str,
    raw_schema: str,
    sumsub_key: str,
    sumsub_secret: str,
    logger: Optional[Any] = None,
    inbox_events_since=dlt.sources.incremental(
        "recorded_at", initial_value=datetime(1970, 1, 1, tzinfo=timezone.utc)
    ),
) -> Iterator[Dict[str, Any]]:
    """
    Fetch applicant data from Sumsub for customers with inbox events since the last run.
    
    Uses PostgreSQL as the source for inbox_events (instead of BigQuery).
    """
    if logger is None:
        logger = logging.getLogger("sumsub_applicants")
    
    start_ts: datetime = inbox_events_since.last_value or datetime(
        1970, 1, 1, tzinfo=timezone.utc
    )
    logger.info("Starting Sumsub applicants sync from %s", start_ts)

    with requests.Session() as session:
        customer_rows: List[Tuple[str, datetime]] = _get_customers_pg(
            dest_connection_string, raw_schema, start_ts
        )
        
        for customer_id, max_recorded_at in customer_rows:
            logger.info(
                "Fetching Sumsub data for customer_id=%s recorded_at=%s",
                customer_id,
                max_recorded_at,
            )

            try:
                resp = _get_applicant_data(
                    session, customer_id, sumsub_key, sumsub_secret
                )
                resp.raise_for_status()
            except RequestException as e:
                logger.warning(
                    "Applicant fetch failed for customer_id=%s (will retry next run): %s",
                    customer_id,
                    e,
                )
                break

            try:
                resp_json = resp.json()
            except ValueError as e:
                logger.warning(
                    "Invalid JSON from Sumsub for customer_id=%s (will retry next run): %s",
                    customer_id,
                    e,
                )
                break

            content_text = resp.text
            document_images: List[Dict[str, Optional[str]]] = []

            applicant_id = resp_json.get("id")
            inspection_id = resp_json.get("inspectionId")

            if applicant_id:
                try:
                    metadata = _get_document_metadata(
                        session, applicant_id, sumsub_key, sumsub_secret
                    )
                except RequestException as e:
                    logger.warning(
                        "Metadata fetch failed for customer_id=%s (continuing without images): %s",
                        customer_id,
                        e,
                    )
                    metadata = {"items": []}

                for item in metadata.get("items", []):
                    image_id = item.get("id")
                    if image_id and inspection_id:
                        try:
                            base64_image = _download_document_image(
                                session,
                                inspection_id,
                                image_id,
                                sumsub_key,
                                sumsub_secret,
                            )
                        except RequestException as e:
                            logger.warning(
                                "Image download failed for customer_id=%s image_id=%s: %s",
                                customer_id,
                                image_id,
                                e,
                            )
                            base64_image = None
                        document_images.append(
                            {"image_id": image_id, "base64_image": base64_image}
                        )

            yield {
                "customer_id": customer_id,
                "recorded_at": max_recorded_at,
                "content": content_text,
                "document_images": document_images,
            }
