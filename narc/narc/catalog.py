"""Catalog model, catalog construction and source-identity binding.

A catalog is an immutable JSON sidecar describing *one* source archive.
It stores structural metadata and per-block summaries/anchors, but never a
full decoded copy of the data.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
from dataclasses import asdict, dataclass, field

from . import format as fmt
from .io_utils import TrackedReader, hash_file


class CatalogError(ValueError):
    """Raised when an archive is structurally unusable for cataloging."""


@dataclass
class Block:
    block_id: str
    fragment_id: int
    column_id: int
    kind: int
    start_row: int
    end_row: int          # half-open, global sample coordinates
    offset: int           # payload offset inside the source file
    length: int           # payload length in bytes
    digest: str           # hex SHA-256 of the payload
    status: str           # "readable" | "damaged"
    damage_reason: str | None
    first_absolute_local: int | None   # index of first absolute record, or None
    carry_in: int | None               # trusted value entering the block, or None
    last_value: int | None             # last reconstructible value, or None
    prefix_missing_locals: list[int]   # explicit missings before first absolute
    checkpoints: dict[str, int] = field(default_factory=dict)

    @property
    def header_offset(self) -> int:
        return self.offset - fmt.BLOCK_HEADER_SIZE

    @property
    def end_offset(self) -> int:
        return self.offset + self.length

    def to_json(self) -> dict:
        data = asdict(self)
        data["kind"] = fmt.KIND_NAMES[self.kind]
        return data

    @classmethod
    def from_json(cls, data: dict) -> "Block":
        data = dict(data)
        data["kind"] = fmt.NAME_TO_KIND[data["kind"]]
        data["checkpoints"] = {int(k): v for k, v in data["checkpoints"].items()}
        return cls(**data)


@dataclass
class Fragment:
    fragment_id: int
    source_name: str
    start_row: int
    end_row: int
    columns: list[int]

    def to_json(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json(cls, data: dict) -> "Fragment":
        return cls(**data)


# ---------------------------------------------------------------------------
# Record-level block decoding (catalog time; full block is already in memory)
# ---------------------------------------------------------------------------

def _decode_block_payload(
    payload: bytes,
    start_row: int,
    record_count: int,
) -> tuple[list[tuple[int, int | None, int | None]], str | None, int | None,
           dict[int, int], int | None, list[int]]:
    """Decode one full payload (cataloging may read the whole archive).

    Returns ``(records, damage_reason, first_absolute_local, checkpoints,
    last_value, prefix_missing_locals)``.  The catalog stores only summary
    scalars and sparse indices, never the full decoded row stream.
    """
    if len(payload) != record_count * fmt.RECORD_SIZE:
        return [], "declared payload length does not match record count", \
            None, {}, None, []

    records: list[tuple[int, int | None, int | None]] = []
    damage_reason: str | None = None
    first_absolute_local: int | None = None
    checkpoints: dict[int, int] = {}
    prefix_missing_locals: list[int] = []
    running: int | None = None
    last_value: int | None = None

    for index in range(record_count):
        base = index * fmt.RECORD_SIZE
        tag = payload[base]
        int_bytes = payload[base + 1:base + 9]
        if tag == fmt.REC_MISSING:
            if int_bytes != b"\x00" * 8:
                damage_reason = damage_reason or "non-zero payload on missing record"
            records.append((index, fmt.REC_MISSING, None))
            if first_absolute_local is None:
                prefix_missing_locals.append(index)
            # Missing does not change the last effective value.
        elif tag in (fmt.REC_ABSOLUTE, fmt.REC_DELTA):
            value = struct.unpack(">q", int_bytes)[0]
            records.append((index, tag, value))
            if tag == fmt.REC_ABSOLUTE:
                if first_absolute_local is None:
                    first_absolute_local = index
                running = value
            elif running is not None:
                running += value
        else:
            damage_reason = damage_reason or f"unknown record tag 0x{tag:02x}"
            records.append((index, None, None))

        # Sparse checkpoint: the exact reconstructed value at this local
        # position (only when a trusted absolute is in scope).  Absolute
        # positions are always recorded so any window can find an anchor.
        if tag == fmt.REC_ABSOLUTE:
            checkpoints[index] = running
        elif (
            running is not None
            and index % fmt.CHECKPOINT_STRIDE == 0
            and first_absolute_local is not None
            and index > first_absolute_local
        ):
            checkpoints[index] = running
        last_value = running

    # carry_in is decided per-chain during structural validation, not here.
    return (records, damage_reason, first_absolute_local,
            checkpoints, last_value, prefix_missing_locals)


# ---------------------------------------------------------------------------
# Catalog construction
# ---------------------------------------------------------------------------

def _read_exact(reader: TrackedReader, offset: int, length: int) -> bytes:
    try:
        data = reader.read(offset, length)
    except OSError as exc:
        raise CatalogError(f"truncated archive at offset {offset}: {exc}")
    if len(data) != length:
        raise CatalogError(f"truncated archive at offset {offset}")
    return data


def build_catalog(
    archive_path: str,
    catalog_path: str,
    source_name: str | None = None,
) -> dict:
    """Read the whole archive and write its JSON catalog.

    Cataloging is the one operation allowed to read a full source.  It does
    not retain decoded rows; the identity hash is recomputed from the file
    itself after structural parsing.
    """
    real_size = os.path.getsize(archive_path)

    with TrackedReader(archive_path) as reader:
        header = _read_exact(reader, 0, fmt.FILE_HEADER_SIZE)
        magic, bom, version = struct.unpack(fmt.F_FILE_HEADER, header)
        if magic != fmt.MAGIC:
            raise CatalogError("bad magic; not a narc archive")
        if bom != fmt.BYTE_ORDER_MARK:
            raise CatalogError("foreign byte order; archive must be big-endian")
        if version != fmt.FORMAT_VERSION:
            raise CatalogError(f"unsupported archive version {version}")

        offset = fmt.FILE_HEADER_SIZE
        current_fragment: int | None = None
        fragments_raw: list[dict] = []
        blocks: list[Block] = []
        eof_seen = False

        while offset < real_size:
            tag_byte = _read_exact(reader, offset, 1)
            tag = tag_byte[0]
            if tag == fmt.END_OF_ARCHIVE:
                rest = _read_exact(reader, offset + 1, fmt.END_OF_ARCHIVE_SIZE - 1)
                (flags,) = struct.unpack(">B", rest)
                if flags != 0:
                    raise CatalogError("invalid end-of-archive flags")
                offset += fmt.END_OF_ARCHIVE_SIZE
                if offset != real_size:
                    raise CatalogError("trailing bytes after end-of-archive marker")
                eof_seen = True
                break

            if tag == fmt.FRAGMENT_TAG:
                rest = _read_exact(
                    reader, offset + 1, fmt.FRAGMENT_HEADER_SIZE - 1
                )
                header = tag_byte + rest
                (
                    header_tag,
                    flags,
                    fragment_id,
                    source_raw,
                    start_row,
                    end_row,
                ) = struct.unpack(fmt.F_FRAGMENT_HEADER, header)
                assert header_tag == fmt.FRAGMENT_TAG
                if flags != 0:
                    raise CatalogError("invalid fragment flags")
                if end_row <= start_row:
                    raise CatalogError(
                        f"fragment {fragment_id} has empty/backwards row range"
                    )
                name = source_raw.rstrip(b"\x00").decode("utf-8")
                fragments_raw.append(
                    {
                        "fragment_id": fragment_id,
                        "source_name": name,
                        "start_row": start_row,
                        "end_row": end_row,
                        "columns": [],
                    }
                )
                current_fragment = fragment_id
                offset += fmt.FRAGMENT_HEADER_SIZE
                continue

            if tag == fmt.BLOCK_TAG:
                if current_fragment is None:
                    raise CatalogError("block outside any fragment")
                block_start = offset
                rest = _read_exact(
                    reader, offset + 1, fmt.BLOCK_HEADER_SIZE - 1
                )
                header = tag_byte + rest
                (
                    header_tag,
                    kind,
                    column_id,
                    record_count,
                    start_row,
                    end_row,
                    payload_offset,
                    payload_length,
                    digest_raw,
                    reserved,
                ) = struct.unpack(fmt.F_BLOCK_HEADER, header)
                assert header_tag == fmt.BLOCK_TAG
                if reserved != 0:
                    raise CatalogError("reserved block field must be zero")
                if kind not in fmt.KIND_NAMES:
                    raise CatalogError(
                        f"unknown block kind {kind} at offset {block_start}"
                    )
                if end_row - start_row != record_count:
                    raise CatalogError(
                        f"block at {block_start}: row range does not match count"
                    )
                if payload_offset != block_start + fmt.BLOCK_HEADER_SIZE:
                    raise CatalogError(
                        f"block at {block_start}: payload offset mismatch"
                    )
                payload: bytes | None
                truncation: str | None = None
                available = max(reader.size - payload_offset, 0)
                if payload_offset > reader.size or available < payload_length:
                    payload = b""
                    truncation = (
                        f"truncated block payload: declared {payload_length} "
                        f"bytes, {available} available"
                    )
                else:
                    payload = _read_exact(reader, payload_offset, payload_length)

                declared_digest = digest_raw.hex()
                actual_digest = hashlib.sha256(payload).hexdigest()
                digest_ok = declared_digest == actual_digest
                length_ok = payload_length == record_count * fmt.RECORD_SIZE

                (
                    records,
                    row_damage,
                    first_absolute_local,
                    checkpoints,
                    last_value,
                    prefix_missing_locals,
                ) = _decode_block_payload(payload, start_row, record_count)
                del records  # catalog keeps summaries, not decoded rows

                reason: str | None = None
                if truncation:
                    reason = truncation
                elif row_damage:
                    # Illegal rows poison the block even if the digest also
                    # happens to fail; keep the structural reason specific.
                    reason = row_damage
                elif not length_ok:
                    reason = "truncated block payload"
                elif not digest_ok:
                    reason = "payload digest mismatch"

                block_id = f"f{current_fragment:04d}.c{column_id:04d}.r{start_row:010d}"
                blocks.append(
                    Block(
                        block_id=block_id,
                        fragment_id=current_fragment,
                        column_id=column_id,
                        kind=kind,
                        start_row=start_row,
                        end_row=end_row,
                        offset=payload_offset,
                        length=payload_length,
                        digest=actual_digest,
                        status="damaged" if reason else "readable",
                        damage_reason=reason,
                        first_absolute_local=first_absolute_local,
                        carry_in=None,  # set per-chain in _validate_structure
                        last_value=last_value,
                        prefix_missing_locals=prefix_missing_locals,
                        checkpoints=checkpoints,
                    )
                )
                # A truncated block still records its declared extent so the
                # catalog can map its rows; parsing then stops at EOF.
                offset = min(payload_offset + payload_length, real_size)
                continue

            raise CatalogError(f"unknown structural tag 0x{tag:02x} at {offset}")

        # Reaching EOF without the end-of-archive marker is acceptable only
        # when the final block was recorded as damaged by truncation.
        if offset < real_size:
            raise CatalogError("parser stopped before the end of the archive")
        if not eof_seen and not any(
            block.status == "damaged" for block in blocks
        ):
            raise CatalogError("missing end-of-archive marker (truncated file)")

        if reader.ledger.bytes_read > real_size:
            raise CatalogError("cataloging read past the end of the archive")

    identity, identity_size = hash_file(archive_path)
    if identity_size != real_size:
        raise CatalogError("source changed while cataloging")

    fragments = _validate_structure(fragments_raw, blocks)

    catalog = {
        "catalog_format": 1,
        "archive_format": fmt.FORMAT_VERSION,
        "source": {
            "path": os.path.abspath(archive_path),
            "name": source_name
            or os.path.basename(os.path.abspath(archive_path)),
            "size": real_size,
            "sha256": identity,
        },
        "fragments": [fragment.to_json() for fragment in fragments],
        "blocks": [block.to_json() for block in blocks],
    }
    with open(catalog_path, "w", encoding="utf-8") as handle:
        json.dump(catalog, handle, indent=2, sort_keys=True)
        handle.write("\n")
    return catalog


def _validate_structure(
    fragments_raw: list[dict], blocks: list[Block]
) -> list[Fragment]:
    if not fragments_raw:
        raise CatalogError("archive contains no fragments")
    fragment_ids = [item["fragment_id"] for item in fragments_raw]
    if len(set(fragment_ids)) != len(fragment_ids):
        raise CatalogError("duplicate fragment ids")

    by_fragment: dict[int, list[Block]] = {}
    for block in blocks:
        by_fragment.setdefault(block.fragment_id, []).append(block)

    fragments: list[Fragment] = []
    for item in fragments_raw:
        fid = item["fragment_id"]
        group = by_fragment.get(fid, [])
        columns = sorted({block.column_id for block in group})
        item["columns"] = columns
        fragments.append(Fragment(**item))

        by_column: dict[int, list[Block]] = {}
        for block in group:
            by_column.setdefault(block.column_id, []).append(block)

        for column_id, chain in by_column.items():
            chain.sort(key=lambda block: block.start_row)
            previous: Block | None = None
            for block in chain:
                if previous is not None:
                    if block.start_row < previous.end_row:
                        raise CatalogError(
                            f"overlapping blocks for fragment {fid} "
                            f"column {column_id}"
                        )
                    if (
                        block.kind == fmt.KIND_DEPENDENT
                        and previous.end_row != block.start_row
                    ):
                        raise CatalogError(
                            f"dependent block {block.block_id} must continue "
                            "the immediately preceding block of the same "
                            "fragment and column"
                        )
                if (
                    block.kind == fmt.KIND_DEPENDENT
                    and previous is None
                ):
                    raise CatalogError(
                        f"dependent block {block.block_id} has no predecessor"
                    )

                if block.kind == fmt.KIND_INDEPENDENT:
                    block.carry_in = None
                elif previous is None or previous.status == "damaged":
                    # A damaged predecessor provides no trusted prior value;
                    # downstream blocks may only use their own absolutes.
                    block.carry_in = None
                else:
                    block.carry_in = previous.last_value
                previous = block
    return fragments


# ---------------------------------------------------------------------------
# Loading and identity binding
# ---------------------------------------------------------------------------

def load_catalog(catalog_path: str) -> dict:
    with open(catalog_path, "r", encoding="utf-8") as handle:
        catalog = json.load(handle)
    catalog["_blocks"] = [Block.from_json(item) for item in catalog["blocks"]]
    return catalog


def verify_identity(
    catalog: dict, archive_path: str | None = None, *, full: bool = False
) -> None:
    """Bind the catalog to its immutable source.

    The cheap check (size only) runs on every query.  ``full=True`` re-hashes
    the whole file and is used by the explicit ``verify`` command and tests.
    A changed version/content forces re-cataloging.
    """
    source = catalog["source"]
    path = archive_path or source["path"]
    actual_size = os.path.getsize(path)
    if actual_size != source["size"]:
        raise CatalogError(
            "source size changed; the source identity is immutable and the "
            "archive must be re-cataloged"
        )
    if full:
        digest, size = hash_file(path)
        if size != source["size"] or digest != source["sha256"]:
            raise CatalogError(
                "source digest changed; the source identity is immutable and "
                "the archive must be re-cataloged"
            )


def blocks_for(
    catalog: dict, fragment_id: int, column_id: int
) -> list[Block]:
    blocks = [
        block
        for block in catalog["_blocks"]
        if block.fragment_id == fragment_id and block.column_id == column_id
    ]
    blocks.sort(key=lambda block: block.start_row)
    return blocks
