import os
import time
import uuid
from contextlib import contextmanager
from typing import Callable, Dict, Optional, Tuple, Union

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# traceparent for current job's span stored in run tags
JOB_TRACEPARENT_TAG = "otel.job.traceparent"
JOB_SPAN_LOCK_TAG = "otel.job.span_lock"


def init_telemetry():
    """Initialize OpenTelemetry tracer"""
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")

    resource = Resource.create(
        {"service.name": "lana-dw-pg", "service.namespace": "lana"}
    )

    provider = TracerProvider(resource=resource)

    otlp_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    # Send spans immediately for ephemeral runtimes
    provider.add_span_processor(SimpleSpanProcessor(otlp_exporter))

    trace.set_tracer_provider(provider)


tracer = trace.get_tracer(__name__)
_trace_propagator = TraceContextTextMapPropagator()


def trace_callable(
    span_name: str,
    callable: Callable,
    span_attributes: Union[dict, None] = None,
    parent_context: Optional[Context] = None,
):
    """
    Wrapper that traces a callable with OpenTelemetry.
    """

    def traced_wrapper(**kwargs):
        with tracer.start_as_current_span(span_name, context=parent_context) as span:
            if span_attributes:
                for key, value in span_attributes.items():
                    span.set_attribute(key, str(value))

            try:
                result = callable(**kwargs)
                span.set_status(trace.Status(trace.StatusCode.OK))
                return result
            except Exception as e:
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
                raise e

    return traced_wrapper


def get_asset_span_context_and_attrs(
    context: Context, asset_key_str: str
) -> Tuple[Optional[Context], Dict[str, str]]:
    """
    For a given asset execution, compute:
      - parent_context: the job-level span context (if any)
      - attrs: common attributes for the asset span
    """
    attrs: Dict[str, str] = {
        "asset.name": asset_key_str,
        "run.id": context.run_id,
    }

    parent_ctx: Optional[Context] = None
    job_name = _get_job_name(context)

    if job_name and job_name != "__ASSET_JOB":
        parent_ctx = _ensure_job_parent_context(context, job_name)
        attrs["job.name"] = job_name

    return parent_ctx, attrs


def _get_job_name(context) -> Optional[str]:
    return (
        getattr(context, "job_name", None)
        or getattr(getattr(context, "dagster_run", None), "job_name", None)
        or getattr(getattr(context, "run", None), "job_name", None)
    )


def _try_acquire_span_creation_lock(context) -> Optional[str]:
    """Try to acquire the exclusive right to create the job span for this run."""
    claim_id = str(uuid.uuid4())

    try:
        context.instance.add_run_tags(context.run_id, {JOB_SPAN_LOCK_TAG: claim_id})
    except Exception:
        return None

    time.sleep(0.05)

    tags = dict(
        getattr(context.instance.get_run_by_id(context.run_id), "tags", {}) or {}
    )

    return claim_id if tags.get(JOB_SPAN_LOCK_TAG) == claim_id else None


def _wait_for_job_traceparent(context, timeout_seconds: float = 1.0) -> Optional[str]:
    """Wait for another process to create and persist the job traceparent."""
    attempts = int(timeout_seconds / 0.05)

    for _ in range(attempts):
        time.sleep(0.05)
        tags = dict(
            getattr(context.instance.get_run_by_id(context.run_id), "tags", {}) or {}
        )
        if traceparent := tags.get(JOB_TRACEPARENT_TAG):
            return traceparent

    return None


def _ensure_job_parent_context(context, job_name: str) -> Optional[Context]:
    """Ensure there is exactly one job-level span per Dagster run."""
    tags = dict(
        getattr(context.instance.get_run_by_id(context.run_id), "tags", {}) or {}
    )
    if existing := tags.get(JOB_TRACEPARENT_TAG):
        return _context_from_traceparent(existing)

    if claim_id := _try_acquire_span_creation_lock(context):
        with tracer.start_as_current_span(
            job_name,
            attributes={"job.name": job_name, "run.id": context.run_id},
        ):
            traceparent = _current_span_to_traceparent()

        if traceparent:
            context.instance.add_run_tags(
                context.run_id, {JOB_TRACEPARENT_TAG: traceparent}
            )
            return _context_from_traceparent(traceparent)

    if traceparent := _wait_for_job_traceparent(context):
        return _context_from_traceparent(traceparent)

    return None


def _context_from_traceparent(traceparent: str) -> Context:
    return _trace_propagator.extract(carrier={"traceparent": traceparent})


def _current_span_to_traceparent() -> Optional[str]:
    carrier: Dict[str, str] = {}
    _trace_propagator.inject(carrier)
    return carrier.get("traceparent")


@contextmanager
def trace_dbt_batch(context, batch_name: str, selected_keys: list):
    """Context manager for tracing a dbt batch execution with OpenTelemetry."""
    attrs = {
        "dbt.batch_name": batch_name,
        "dbt.model_count": len(selected_keys),
        "dbt.models": selected_keys,
        "run.id": context.run_id,
    }

    parent_ctx: Optional[Context] = None
    job_name = _get_job_name(context)

    if job_name and job_name != "__ASSET_JOB":
        parent_ctx = _ensure_job_parent_context(context, job_name)
        attrs["job.name"] = job_name

    with tracer.start_as_current_span(batch_name, context=parent_ctx) as span:
        for key, value in attrs.items():
            span.set_attribute(key, value)

        try:
            yield span
            span.set_status(trace.Status(trace.StatusCode.OK))
        except Exception as e:
            span.set_status(trace.Status(trace.StatusCode.ERROR, str(e)))
            raise
