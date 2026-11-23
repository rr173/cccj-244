"""narc package: offline cataloging and narrow-window reads."""

from .catalog import build_catalog, load_catalog, verify_identity
from .query import Cell, QueryResult, query_window

__all__ = [
    "build_catalog",
    "load_catalog",
    "verify_identity",
    "query_window",
    "Cell",
    "QueryResult",
]
