"""Wire format for the UDP ARQ lab (owner: Satwik).

Frozen 16-byte header, big-endian (``!``) struct packing, payload appended:

    offset  size  field        type     meaning
    0       2     checksum     uint16   one's-complement checksum (zero while computing)
    2       4     seq          uint32   sequence number of this packet
    6       4     ack          uint32   next expected sequence number (cumulative ACK)
    10      1     flags        uint8    DATA=0x01 ACK=0x02 FIN=0x04
    11      1     version      uint8    must equal PROTOCOL_VERSION
    12      2     payload_len  uint16   bytes following the header
    14      2     magic        uint16   must equal MAGIC

struct format string: ``!HIIBBHH`` (16 bytes). Payload may be 0..MAX_PAYLOAD
bytes and is appended verbatim after the header.

``decode()`` raises ``PacketError`` for any buffer that is truncated, has a
wrong magic or version, uses unknown flag bits, or whose ``payload_len``
disagrees with the actual remaining length. Non-bytes input raises
``TypeError``.
"""

from __future__ import annotations

import struct
from dataclasses import dataclass

HEADER_STRUCT = "!HIIBBHH"
HEADER_SIZE = struct.calcsize(HEADER_STRUCT)  # == 16

PROTOCOL_VERSION = 1
MAGIC = 0xA55A

FLAG_DATA = 0x01
FLAG_ACK = 0x02
FLAG_FIN = 0x04
KNOWN_FLAGS = FLAG_DATA | FLAG_ACK | FLAG_FIN

DEFAULT_MSS = 1024
# A packet is header + payload, and a UDP/IPv4 datagram tops out at 65507
# bytes, so the payload can be at most 65507 - HEADER_SIZE.
MAX_PAYLOAD = 65507 - HEADER_SIZE


class PacketError(ValueError):
    """Raised when a byte buffer is not a valid UDP-ARQ packet."""


@dataclass(frozen=True)
class Header:
    """The fixed 16-byte packet header (all fields big-endian on the wire)."""

    seq: int = 0
    ack: int = 0
    flags: int = FLAG_DATA
    version: int = PROTOCOL_VERSION
    payload_len: int = 0
    checksum: int = 0

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        if not 0 <= self.seq <= 0xFFFFFFFF:
            raise PacketError(f"seq out of range: {self.seq}")
        if not 0 <= self.ack <= 0xFFFFFFFF:
            raise PacketError(f"ack out of range: {self.ack}")
        if self.flags & ~KNOWN_FLAGS:
            raise PacketError(f"unknown flag bits: {self.flags:#04x}")
        if self.version != PROTOCOL_VERSION:
            raise PacketError(f"unsupported version: {self.version}")
        if not 0 <= self.payload_len <= MAX_PAYLOAD:
            raise PacketError(f"payload_len out of range: {self.payload_len}")
        if not 0 <= self.checksum <= 0xFFFF:
            raise PacketError(f"checksum out of range: {self.checksum}")


def encode(header: Header, payload: bytes | bytearray | memoryview = b"") -> bytes:
    """Serialize ``header`` + ``payload`` into a single packet.

    Raises ``PacketError`` if ``header.payload_len`` does not match
    ``len(payload)``.
    """
    if not isinstance(header, Header):
        raise TypeError(f"header must be a Header, got {type(header).__name__}")
    payload = bytes(payload)
    header.validate()
    if header.payload_len != len(payload):
        raise PacketError(
            f"header.payload_len {header.payload_len} != actual payload length {len(payload)}"
        )
    return (
        struct.pack(
            HEADER_STRUCT,
            header.checksum,
            header.seq,
            header.ack,
            header.flags,
            header.version,
            header.payload_len,
            MAGIC,
        )
        + payload
    )


def decode(buf: bytes | bytearray | memoryview) -> tuple[Header, bytes]:
    """Parse a packet into ``(header, payload)``.

    Raises ``PacketError`` for truncated or malformed input and
    ``TypeError`` for non-bytes-like input.
    """
    if not isinstance(buf, (bytes, bytearray, memoryview)):
        raise TypeError(f"expected bytes-like object, got {type(buf).__name__}")
    view = memoryview(buf)
    if len(view) < HEADER_SIZE:
        raise PacketError(
            f"truncated packet: {len(view)} bytes < {HEADER_SIZE}-byte header"
        )
    checksum, seq, ack, flags, version, payload_len, magic = struct.unpack_from(
        HEADER_STRUCT, view
    )
    if magic != MAGIC:
        raise PacketError(f"bad magic {magic:#06x}, expected {MAGIC:#06x}")
    if version != PROTOCOL_VERSION:
        raise PacketError(f"unsupported version {version}, expected {PROTOCOL_VERSION}")
    if flags & ~KNOWN_FLAGS:
        raise PacketError(f"unknown flag bits {flags:#04x}")
    actual_len = len(view) - HEADER_SIZE
    if payload_len != actual_len:
        raise PacketError(
            f"payload_len {payload_len} != actual remaining bytes {actual_len}"
        )
    header = Header(
        seq=seq,
        ack=ack,
        flags=flags,
        version=version,
        payload_len=payload_len,
        checksum=checksum,
    )
    return header, bytes(view[HEADER_SIZE:])
