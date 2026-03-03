#!/usr/bin/env python3
"""
Ad-hoc script to load Sumsub applicant data from known external user IDs.

Usage:
    python scripts/load_sumsub_applicants.py --csv secrets/sumsub_external_user_ids.csv

Environment variables required:
    SUMSUB_KEY        - Sumsub API key
    SUMSUB_SECRET     - Sumsub API secret
    DST_PG_HOST       - Postgres host (default: localhost)
    DST_PG_PORT       - Postgres port (default: 5433)
    DST_PG_DATABASE   - Postgres database (default: lana_dw)
    DST_PG_USER       - Postgres user (default: postgres)
    DST_PG_PASSWORD   - Postgres password (default: postgres)
    DST_RAW_SCHEMA    - Raw schema name (default: raw)
"""

from __future__ import annotations

import argparse
import base64
import csv
import hashlib
import hmac
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
from sqlalchemy import create_engine, text

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

REQUEST_TIMEOUT = 60
SUMSUB_API_BASE = "https://api.sumsub.com"
SUMSUB_APPLICANTS_DLT_TABLE = "sumsub_applicants_dlt"


def get_connection_string() -> str:
    """Build PostgreSQL connection string from environment variables."""
    host = os.getenv("DST_PG_HOST", "localhost")
    port = os.getenv("DST_PG_PORT", "5433")
    database = os.getenv("DST_PG_DATABASE", "lana_dw")
    user = os.getenv("DST_PG_USER", "postgres")
    password = os.getenv("DST_PG_PASSWORD", "postgres")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


def get_raw_schema() -> str:
    """Get raw schema name from environment."""
    return os.getenv("DST_RAW_SCHEMA", "raw")


def ensure_table_exists(connection_string: str, schema: str) -> None:
    """Create the sumsub_applicants_dlt table if it doesn't exist."""
    engine = create_engine(connection_string)
    
    sql = text(f"""
        CREATE TABLE IF NOT EXISTS {schema}.{SUMSUB_APPLICANTS_DLT_TABLE} (
            customer_id TEXT NOT NULL,
            recorded_at TIMESTAMPTZ NOT NULL,
            content TEXT,
            document_images JSONB,
            _dlt_load_id TEXT,
            _dlt_id TEXT,
            PRIMARY KEY (customer_id, recorded_at)
        )
    """)
    
    with engine.connect() as conn:
        conn.execute(sql)
        conn.commit()
    
    logger.info(f"Table {schema}.{SUMSUB_APPLICANTS_DLT_TABLE} ready")


def sumsub_send(
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


def get_applicant_data(
    session: requests.Session, external_user_id: str, key: str, secret: str
) -> requests.Response:
    """Fetch applicant data by external user ID."""
    url = f"{SUMSUB_API_BASE}/resources/applicants/-;externalUserId={external_user_id}/one"
    return sumsub_send(session, "GET", url, key, secret)


def get_document_metadata(
    session: requests.Session, applicant_id: str, key: str, secret: str
) -> Dict[str, Any]:
    """Fetch document metadata for an applicant."""
    url = f"{SUMSUB_API_BASE}/resources/applicants/{applicant_id}/metadata/resources"
    resp = sumsub_send(session, "GET", url, key, secret)
    resp.raise_for_status()
    return resp.json()


def download_document_image(
    session: requests.Session, inspection_id: str, image_id: str, key: str, secret: str
) -> Optional[str]:
    """Download a document image and return as base64."""
    url = f"{SUMSUB_API_BASE}/resources/inspections/{inspection_id}/resources/{image_id}"
    resp = sumsub_send(session, "GET", url, key, secret)
    if resp.status_code == 200:
        return base64.b64encode(resp.content).decode("utf-8")
    return None


def load_external_user_ids(csv_path: str) -> List[str]:
    """Load external user IDs from CSV file."""
    ids = []
    with open(csv_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Support both column names
            ext_id = row.get("sumsub_external_user_id", "").strip()
            if not ext_id:
                ext_id = row.get("external_user_id", "").strip()
            if ext_id and not ext_id.startswith("#"):
                ids.append(ext_id)
    return ids


def insert_applicant(
    connection_string: str,
    schema: str,
    customer_id: str,
    content: str,
    document_images: List[Dict[str, Any]],
) -> None:
    """Insert or update an applicant record."""
    engine = create_engine(connection_string)
    
    now = datetime.now(timezone.utc)
    load_id = str(int(now.timestamp()))
    
    sql = text(f"""
        INSERT INTO {schema}.{SUMSUB_APPLICANTS_DLT_TABLE} 
            (customer_id, recorded_at, content, document_images, _dlt_load_id, _dlt_id)
        VALUES 
            (:customer_id, :recorded_at, :content, :document_images, :load_id, :dlt_id)
        ON CONFLICT (customer_id, recorded_at) 
        DO UPDATE SET
            content = EXCLUDED.content,
            document_images = EXCLUDED.document_images,
            _dlt_load_id = EXCLUDED._dlt_load_id
    """)
    
    with engine.connect() as conn:
        conn.execute(sql, {
            "customer_id": customer_id,
            "recorded_at": now,
            "content": content,
            "document_images": json.dumps(document_images),
            "load_id": load_id,
            "dlt_id": f"{customer_id}_{load_id}",
        })
        conn.commit()


def fetch_and_load_applicant(
    session: requests.Session,
    external_user_id: str,
    sumsub_key: str,
    sumsub_secret: str,
    connection_string: str,
    schema: str,
    skip_images: bool = False,
) -> bool:
    """Fetch applicant data from Sumsub and load into database."""
    logger.info(f"Fetching data for external_user_id={external_user_id}")
    
    try:
        resp = get_applicant_data(session, external_user_id, sumsub_key, sumsub_secret)
        resp.raise_for_status()
    except requests.RequestException as e:
        if hasattr(e, 'response') and e.response is not None and e.response.status_code == 404:
            logger.warning(f"Applicant not found in Sumsub: {external_user_id}")
            return False
        logger.error(f"Failed to fetch applicant {external_user_id}: {e}")
        return False
    
    try:
        resp_json = resp.json()
    except ValueError as e:
        logger.error(f"Invalid JSON response for {external_user_id}: {e}")
        return False
    
    content_text = resp.text
    document_images: List[Dict[str, Optional[str]]] = []
    
    # Fetch document images if not skipped
    if not skip_images:
        applicant_id = resp_json.get("id")
        inspection_id = resp_json.get("inspectionId")
        
        if applicant_id:
            try:
                metadata = get_document_metadata(session, applicant_id, sumsub_key, sumsub_secret)
            except requests.RequestException as e:
                logger.warning(f"Failed to fetch metadata for {external_user_id}: {e}")
                metadata = {"items": []}
            
            for item in metadata.get("items", []):
                image_id = item.get("id")
                if image_id and inspection_id:
                    try:
                        base64_image = download_document_image(
                            session, inspection_id, image_id, sumsub_key, sumsub_secret
                        )
                    except requests.RequestException as e:
                        logger.warning(f"Failed to download image {image_id}: {e}")
                        base64_image = None
                    document_images.append({
                        "image_id": image_id,
                        "base64_image": base64_image,
                    })
    
    # Use external_user_id as customer_id
    insert_applicant(
        connection_string=connection_string,
        schema=schema,
        customer_id=external_user_id,
        content=content_text,
        document_images=document_images,
    )
    
    logger.info(f"Loaded applicant: {external_user_id}")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Load Sumsub applicant data from known external user IDs"
    )
    parser.add_argument(
        "--csv",
        required=True,
        help="Path to CSV file with external user IDs (column: sumsub_external_user_id or external_user_id)",
    )
    parser.add_argument(
        "--skip-images",
        action="store_true",
        help="Skip downloading document images",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without actually loading",
    )
    args = parser.parse_args()
    
    # Check required env vars
    sumsub_key = os.getenv("SUMSUB_KEY")
    sumsub_secret = os.getenv("SUMSUB_SECRET")
    
    if not sumsub_key or not sumsub_secret:
        logger.error("SUMSUB_KEY and SUMSUB_SECRET environment variables are required")
        sys.exit(1)
    
    # Load external user IDs from CSV
    if not os.path.exists(args.csv):
        logger.error(f"CSV file not found: {args.csv}")
        sys.exit(1)
    
    external_user_ids = load_external_user_ids(args.csv)
    
    if not external_user_ids:
        logger.error("No external user IDs found in CSV")
        sys.exit(1)
    
    logger.info(f"Found {len(external_user_ids)} external user IDs to load")
    
    if args.dry_run:
        logger.info("DRY RUN - would load these IDs:")
        for ext_id in external_user_ids:
            print(f"  - {ext_id}")
        sys.exit(0)
    
    # Setup database
    connection_string = get_connection_string()
    schema = get_raw_schema()
    
    ensure_table_exists(connection_string, schema)
    
    # Fetch and load each applicant
    success_count = 0
    fail_count = 0
    
    with requests.Session() as session:
        for external_user_id in external_user_ids:
            if fetch_and_load_applicant(
                session=session,
                external_user_id=external_user_id,
                sumsub_key=sumsub_key,
                sumsub_secret=sumsub_secret,
                connection_string=connection_string,
                schema=schema,
                skip_images=args.skip_images,
            ):
                success_count += 1
            else:
                fail_count += 1
    
    logger.info(f"Done! Loaded: {success_count}, Failed: {fail_count}")


if __name__ == "__main__":
    main()
