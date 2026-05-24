"""Observability setup for logging, metrics, and tracing."""

import logging
import sys
from typing import Any

from fastapi import FastAPI

from app.config import settings


def setup_logging() -> None:
    """Configure structured JSON logging when structlog is installed."""
    try:
        import structlog
    except Exception:
        logging.basicConfig(level=logging.INFO)
        return

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def setup_tracing(app: FastAPI) -> None:
    """Configure OpenTelemetry tracing when packages and endpoint are available."""
    if not settings.otel_exporter_otlp_endpoint:
        return

    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.instrumentation.botocore import BotocoreInstrumentor
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor

        from app.db import get_engine, is_database_enabled
    except Exception:
        return

    resource = Resource.create({"service.name": "fern-api", "replica.id": settings.replica_id})
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint))
    )
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
    BotocoreInstrumentor().instrument()

    if is_database_enabled():
        try:
            SQLAlchemyInstrumentor().instrument(engine=get_engine())
        except Exception:
            pass


def setup_metrics(app: FastAPI) -> None:
    """Expose Prometheus metrics when instrumentation packages are installed."""
    if not settings.metrics_enabled:
        return

    try:
        from prometheus_client import Gauge
        from prometheus_fastapi_instrumentator import Instrumentator

        from app.db import get_engine, is_database_enabled
    except Exception:
        return

    db_pool_in_use = Gauge("fern_db_pool_in_use", "SQLAlchemy DB connections in use")
    db_pool_size = Gauge("fern_db_pool_size", "SQLAlchemy DB pool size")

    def update_db_pool_metrics(_: Any) -> None:
        if not is_database_enabled():
            db_pool_in_use.set(0)
            db_pool_size.set(0)
            return
        try:
            pool = get_engine().pool
            db_pool_in_use.set(pool.checkedout())
            db_pool_size.set(pool.size())
        except Exception:
            db_pool_in_use.set(0)
            db_pool_size.set(0)

    Instrumentator().add(update_db_pool_metrics).instrument(app).expose(app)


def setup_observability(app: FastAPI) -> None:
    """Configure all optional observability integrations."""
    setup_logging()
    setup_metrics(app)
    setup_tracing(app)
