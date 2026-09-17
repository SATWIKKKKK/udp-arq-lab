"""
Packet integrity utilities for UDP ARQ.

Implements a 16-bit one's-complement checksum.

The checksum is stored in bytes 0-1 of the packet.
"""

from __future__ import annotations


CHECKSUM_SIZE = 2


def checksum(buf: bytes) -> int:
    """
    Calculate the 16-bit one's-complement checksum.

    The checksum field (bytes 0-1) must be zeroed before
    calling this function.

    Args:
        buf: Complete packet with checksum field zeroed.

    Returns:
        16-bit checksum as an integer.
    """

    if not isinstance(buf, (bytes, bytearray, memoryview)):
        raise TypeError("buf must be bytes-like")

    buf = bytes(buf)

    # If the packet has an odd number of bytes,
    # pad it with one zero byte.
    if len(buf) % 2:
        buf += b"\x00"

    total = 0

    # Process the packet as 16-bit words.
    # Network byte order = big-endian.
    for i in range(0, len(buf), 2):
        word = (buf[i] << 8) | buf[i + 1]

        total += word

        # Fold carry into the lower 16 bits.
        total = (total & 0xFFFF) + (total >> 16)

    # Fold any remaining carry.
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)

    # Take one's complement.
    return (~total) & 0xFFFF


def verify(buf: bytes) -> bool:
    """
    Verify the checksum stored in bytes 0-1 of a packet.

    Steps:
        1. Read the received checksum from bytes 0-1.
        2. Zero bytes 0-1.
        3. Recalculate the checksum.
        4. Compare the received and calculated checksums.

    Args:
        buf: Complete packet.

    Returns:
        True if the checksum is valid, otherwise False.
    """

    if not isinstance(buf, (bytes, bytearray, memoryview)):
        raise TypeError("buf must be bytes-like")

    buf = bytes(buf)

    # Packet must contain at least the 2-byte checksum field.
    if len(buf) < CHECKSUM_SIZE:
        return False

    # Read the checksum stored in bytes 0-1.
    expected = int.from_bytes(
        buf[0:2],
        byteorder="big",
    )

    # Make a copy so that the original packet is not modified.
    packet = bytearray(buf)

    # Zero the checksum field.
    packet[0:2] = b"\x00\x00"

    # Recalculate the checksum.
    calculated = checksum(packet)

    # Compare received checksum with calculated checksum.
    return calculated == expected