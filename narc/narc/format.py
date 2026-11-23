"""Wire-format constants for the instrument archive (narc) container.

The format is fixed-length, big-endian and self-describing at the block
level.  Nothing in here depends on a third-party library.
"""

MAGIC = b"INARCV01"
FORMAT_VERSION = 1
BYTE_ORDER_MARK = 0xFEFF  # big-endian BOM; 0xFFFE on disk means foreign endianness

# Top-level structural tags.
FRAGMENT_TAG = 0x46  # 'F'
BLOCK_TAG = 0x42     # 'B'
END_OF_ARCHIVE = 0x45  # 'E'

# Record tags (one byte, followed by a signed 64-bit payload).
REC_MISSING = 0
REC_ABSOLUTE = 1
REC_DELTA = 2

# Block kinds.
KIND_INDEPENDENT = 1
KIND_DEPENDENT = 2

KIND_NAMES = {
    KIND_INDEPENDENT: "independent",
    KIND_DEPENDENT: "dependent",
}
NAME_TO_KIND = {v: k for k, v in KIND_NAMES.items()}

# Every record is exactly one tag byte plus eight payload bytes.
RECORD_SIZE = 9

# Struct format strings (explicit big-endian; the document fixes the byte order).
F_FILE_HEADER = ">8sHH"             # magic, BOM, version
F_FRAGMENT_HEADER = ">BBH16sII"     # tag, flags, fid, source name(16), start, end
F_BLOCK_HEADER = ">BBHIIIII32sI"    # tag, kind, column, count, start, end,
                                    # payload_offset, payload_length, digest, reserved
F_END_OF_ARCHIVE = ">BB"

FILE_HEADER_SIZE = 12
FRAGMENT_HEADER_SIZE = 28
BLOCK_HEADER_SIZE = 60
END_OF_ARCHIVE_SIZE = 2

# Sparse checkpoint stride used inside catalogued blocks (see FORMAT.md).
CHECKPOINT_STRIDE = 256

# Result status tokens returned to callers.
STATUS_VALUE = "value"
STATUS_MISSING = "missing"            # explicit missing record
STATUS_DAMAGED = "damaged"            # block unreadable
STATUS_MISSING_PREFIX = "missing_prefix"  # delta with no trusted prior value
STATUS_UNCOVERED = "uncovered"        # no block owns this row for the column
