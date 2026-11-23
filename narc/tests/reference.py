"""Full-archive reference decoder, written independently from narc.query.

This deliberately re-parses the bytes from scratch with plain struct calls so
that any shared assumption between catalog and query code cannot hide a bug.
"""

from __future__ import annotations

import hashlib
import os
import struct

from narc import format as fmt

MISSING = "missing"
VALUE = "value"
DAMAGED = "damaged"
MISSING_PREFIX = "missing_prefix"
UNCOVERED = "uncovered"


def _parse_headers(path: str):
    data = open(path, "rb").read()
    magic, bom, version = struct.unpack_from(fmt.F_FILE_HEADER, data, 0)
    assert magic == fmt.MAGIC and bom == fmt.BYTE_ORDER_MARK
    assert version == fmt.FORMAT_VERSION
    offset = fmt.FILE_HEADER_SIZE
    fragments = {}
    blocks = []
    current_fragment = None
    while offset < len(data):
        tag = data[offset]
        if tag == fmt.END_OF_ARCHIVE:
            assert data[offset + 1] == 0
            offset += 2
            assert offset == len(data)
            break
        if tag == fmt.FRAGMENT_TAG:
            flags, fid, raw_name, start, end = struct.unpack_from(
                ">BH16sII", data, offset + 1
            )
            assert flags == 0
            name = raw_name.rstrip(b"\x00").decode()
            fragments[fid] = (name, start, end)
            current_fragment = fid
            offset += fmt.FRAGMENT_HEADER_SIZE
            continue
        if tag == fmt.BLOCK_TAG:
            (
                _header_tag, kind, column, count, start, end,
                payload_offset, payload_length, digest, reserved,
            ) = struct.unpack_from(fmt.F_BLOCK_HEADER, data, offset)
            assert reserved == 0 and end - start == count
            assert payload_offset == offset + fmt.BLOCK_HEADER_SIZE
            payload = data[payload_offset:payload_offset + payload_length]
            blocks.append(
                dict(
                    fragment=current_fragment, kind=kind, column=column,
                    start=start, end=end, offset=payload_offset,
                    length=payload_length, count=count, payload=payload,
                    digest=digest.hex(),
                )
            )
            offset = payload_offset + payload_length
            continue
        raise AssertionError(f"unexpected tag {tag}")
    return data, fragments, blocks


def reference_map(path: str) -> dict[tuple[int, int], tuple[str, int | None]]:
    """Return the exact (status, value) truth for every row/column pair."""
    _, _, blocks = _parse_headers(path)
    grouped: dict[tuple[int, int], list[dict]] = {}
    for block in blocks:
        grouped.setdefault((block["fragment"], block["column"]), []).append(block)

    result: dict[tuple[int, int], tuple[str, int | None]] = {}
    for chain in grouped.values():
        chain.sort(key=lambda item: item["start"])
        carry: int | None = None
        for block in chain:
            payload = block["payload"]
            length_bad = block["length"] != block["count"] * fmt.RECORD_SIZE
            digest_bad = (
                hashlib.sha256(payload).hexdigest() != block["digest"]
            )
            illegal = False
            parsed = []
            for index in range(block["count"]):
                base = index * fmt.RECORD_SIZE
                if base + fmt.RECORD_SIZE > len(payload):
                    length_bad = True
                    break
                tag = payload[base]
                value = struct.unpack_from(">q", payload, base + 1)[0]
                if tag == fmt.REC_MISSING:
                    if payload[base + 1:base + 9] != b"\x00" * 8:
                        illegal = True
                    parsed.append((tag, 0))
                elif tag in (fmt.REC_ABSOLUTE, fmt.REC_DELTA):
                    parsed.append((tag, value))
                else:
                    illegal = True
                    parsed.append((tag, value))
            if length_bad or digest_bad or illegal:
                for row in range(block["start"], block["end"]):
                    result[(row, block["column"])] = (DAMAGED, None)
                carry = None  # downstream cannot borrow across a bad prefix
                continue

            running = carry if block["kind"] == fmt.KIND_DEPENDENT else None
            for index, (tag, value) in enumerate(parsed):
                row = block["start"] + index
                if tag == fmt.REC_MISSING:
                    # Missing is an explicit observation and never changes the
                    # running value, even when no anchor exists yet.
                    result[(row, block["column"])] = (MISSING, None)
                elif tag == fmt.REC_ABSOLUTE:
                    running = value
                    result[(row, block["column"])] = (VALUE, value)
                else:
                    if running is None:
                        result[(row, block["column"])] = (MISSING_PREFIX, None)
                    else:
                        running += value
                        result[(row, block["column"])] = (VALUE, running)
            carry = running
    return result


def expected_window(
    path: str, start: int, end: int, columns: list[int]
) -> dict[tuple[int, int], tuple[str, int | None]]:
    truth = reference_map(path)
    output = {}
    for row in range(start, end):
        for column in columns:
            output[(row, column)] = truth.get((row, column), (UNCOVERED, None))
    return output


def file_size(path: str) -> int:
    return os.path.getsize(path)
