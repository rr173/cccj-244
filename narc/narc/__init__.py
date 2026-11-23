"""narc package: offline cataloging and narrow-window reads."""

from .catalog import build_catalog, load_catalog, verify_identity
from .gap_ledger import apply_batch as gap_apply_batch
from .gap_ledger import init_ledger as gap_init_ledger
from .gap_ledger import pending_intervals as gap_pending_intervals
from .gap_model import GapError
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
    "gap_init_ledger",
    "gap_apply_batch",
    "gap_pending_intervals",
    "GapError",
]
