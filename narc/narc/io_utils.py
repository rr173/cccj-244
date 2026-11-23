"""Byte-accounted random access over an immutable archive source."""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field


@dataclass
class ReadSpan:
    """One physical read against the underlying file."""

    offset: int
    length: int

    @property
    def end(self) -> int:
        return self.offset + self.length


@dataclass
class ReadLedger:
    """Audit trail of every byte read from the archive."""

    spans: list[ReadSpan] = field(default_factory=list)
    bytes_read: int = 0

    def record(self, offset: int, length: int) -> None:
        self.spans.append(ReadSpan(offset, length))
        self.bytes_read += length

    def touches(self, offset: int, length: int) -> bool:
        end = offset + length
        return any(not (span.end <= offset or span.offset >= end) for span in self.spans)

    def assert_within(self, allowed: list[tuple[int, int]]) -> None:
        """Assert every logged span is fully contained in an allowed range."""
        for span in self.spans:
            if not any(
                start <= span.offset and span.end <= end for start, end in allowed
            ):
                raise AssertionError(
                    f"read {span.offset}:{span.end} escapes allowed ranges {allowed}"
                )


class TrackedReader:
    """pread-based reader that refuses reads outside declared ranges.

    ``allowed`` is a list of inclusive-offset (start, end-exclusive) tuples.
    Queries pass exactly the payload ranges of the blocks they need, so an
    accidental scan into another column or the file head fails loudly.
    """

    def __init__(
        self,
        path: str,
        allowed: list[tuple[int, int]] | None = None,
        ledger: ReadLedger | None = None,
    ) -> None:
        self.path = path
        self.allowed = sorted(allowed or [(0, os.path.getsize(path))])
        self.ledger = ledger or ReadLedger()
        self._fd = os.open(path, os.O_RDONLY)
        self.size = os.fstat(self._fd).st_size

    def close(self) -> None:
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def __enter__(self) -> "TrackedReader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _check_range(self, offset: int, length: int) -> None:
        end = offset + length
        if offset < 0 or end > self.size:
            raise OSError(f"read {offset}:{end} outside file size {self.size}")
        for start, stop in self.allowed:
            if start <= offset and end <= stop:
                return
        raise PermissionError(
            f"read {offset}:{end} escapes the permitted ranges {self.allowed}"
        )

    def read(self, offset: int, length: int) -> bytes:
        if length == 0:
            return b""
        self._check_range(offset, length)
        chunks: list[bytes] = []
        remaining = length
        cursor = offset
        while remaining:
            chunk = os.pread(self._fd, remaining, cursor)
            if not chunk:
                raise OSError(
                    f"short read at {cursor}: wanted {remaining} more bytes"
                )
            chunks.append(chunk)
            cursor += len(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        self.ledger.record(offset, length)
        return data


def hash_file(path: str) -> tuple[str, int]:
    """Full-file SHA-256 used as the immutable source identity."""
    digest = hashlib.sha256()
    size = 0
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1 << 20)
            if not chunk:
                break
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size
