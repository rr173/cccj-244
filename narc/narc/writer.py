"""Minimal archive writer used to build fixtures and example data.

Production cataloging never uses this module; it exists so tests can emit
exact bytes and inject truncation / digest / illegal-row corruption.
"""

from __future__ import annotations

import hashlib
import os
import struct

from . import format as fmt

_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


def _check_int64(value: int) -> int:
    if not isinstance(value, int) or not (_INT64_MIN <= value <= _INT64_MAX):
        raise ValueError(f"{value!r} is not a signed 64-bit integer")
    return value


def encode_record(tag: int, value: int = 0) -> bytes:
    if tag == fmt.REC_MISSING:
        if value != 0:
            raise ValueError("missing records must carry zero payload")
        return bytes([tag]) + b"\x00" * 8
    if tag in (fmt.REC_ABSOLUTE, fmt.REC_DELTA):
        return bytes([tag]) + struct.pack(">q", _check_int64(value))
    raise ValueError(f"unknown record tag {tag}")


class ArchiveWriter:
    def __init__(self, path: str) -> None:
        self.path = path
        self._handle = open(path, "wb")
        self._handle.write(
            struct.pack(
                fmt.F_FILE_HEADER, fmt.MAGIC, fmt.BYTE_ORDER_MARK,
                fmt.FORMAT_VERSION,
            )
        )
        self._fragment_ids: set[int] = set()
        self._last_block_key: tuple[int, int] | None = None
        self._last_block_end: int | None = None

    def close(self) -> None:
        self._handle.write(struct.pack(fmt.F_END_OF_ARCHIVE, fmt.END_OF_ARCHIVE, 0))
        self._handle.close()

    def __enter__(self) -> "ArchiveWriter":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def fragment(
        self,
        fragment_id: int,
        source_name: str,
        start_row: int,
        end_row: int,
    ) -> None:
        if fragment_id in self._fragment_ids:
            raise ValueError(f"duplicate fragment id {fragment_id}")
        if end_row <= start_row:
            raise ValueError("fragment row range must be non-empty")
        name = source_name.encode("utf-8")
        if len(name) > 16:
            raise ValueError("source name must fit in 16 bytes")
        self._fragment_ids.add(fragment_id)
        self._handle.write(
            struct.pack(
                fmt.F_FRAGMENT_HEADER,
                fmt.FRAGMENT_TAG, 0, fragment_id,
                name.ljust(16, b"\x00"), start_row, end_row,
            )
        )
        self._last_block_key = None
        self._last_block_end = None

    def block(
        self,
        kind: int,
        column_id: int,
        start_row: int,
        records: list[tuple[int, int]],
    ) -> None:
        if kind not in fmt.KIND_NAMES:
            raise ValueError("unknown block kind")
        payload = b"".join(encode_record(tag, value) for tag, value in records)
        count = len(records)
        end_row = start_row + count
        payload_offset = self._handle.tell() + fmt.BLOCK_HEADER_SIZE
        digest = hashlib.sha256(payload).digest()
        self._handle.write(
            struct.pack(
                fmt.F_BLOCK_HEADER,
                fmt.BLOCK_TAG, kind, column_id, count, start_row, end_row,
                payload_offset, len(payload), digest, 0,
            )
        )
        self._handle.write(payload)

        key = (self._current_fragment(), column_id)
        if kind == fmt.KIND_DEPENDENT:
            if self._last_block_key != key:
                raise ValueError(
                    "dependent block must continue the immediately preceding "
                    "block of the same fragment and column"
                )
            if self._last_block_end != start_row:
                raise ValueError("dependent block is not contiguous with its "
                                 "predecessor")
        self._last_block_key = key
        self._last_block_end = end_row

    def _current_fragment(self) -> int:
        if not self._fragment_ids:
            raise ValueError("block written before any fragment")
        return max(self._fragment_ids)


def patch_bytes(path: str, offset: int, data: bytes) -> None:
    """Overwrite bytes in an existing archive (test corruption injection)."""
    with open(path, "r+b") as handle:
        handle.seek(offset, os.SEEK_SET)
        handle.write(data)


def truncate(path: str, new_size: int) -> None:
    with open(path, "r+b") as handle:
        handle.truncate(new_size)
