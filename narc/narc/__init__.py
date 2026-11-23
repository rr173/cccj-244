"""narc package: offline cataloging and narrow-window reads."""

from .catalog import build_catalog, load_catalog, verify_identity
from .query import Cell, QueryResult, query_window
from .release import (
    Authorization,
    ReleaseError,
    parse_authorization,
    release_archive,
)

__all__ = [
    "build_catalog",
    "load_catalog",
    "verify_identity",
    "query_window",
    "Cell",
    "QueryResult",
    "release_archive",
    "parse_authorization",
    "Authorization",
    "ReleaseError",
]
