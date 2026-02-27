"""DLT resources for data extraction."""

from src.dlt_resources.postgres import create_dlt_postgres_resource
from src.dlt_resources.bitfinex import ticker, trades, order_book
from src.dlt_resources.sumsub import applicants, SUMSUB_APPLICANTS_DLT_TABLE

__all__ = [
    "create_dlt_postgres_resource",
    "ticker",
    "trades",
    "order_book",
    "applicants",
    "SUMSUB_APPLICANTS_DLT_TABLE",
]
