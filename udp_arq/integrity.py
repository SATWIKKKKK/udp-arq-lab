"""
Packet integrity utilities for UDP ARQ.

Implements a 16-bit one's-complement checksum.

The checksum is calculated over the complete packet with the
checksum field zeroed before calculation.
"""

from __future__ import annotations


CHECKSUM_SIZE = 2


def checksum(data: bytes) -> int:
    """
    Calculate the 16-bit one's-complement checksum of data.

    The input should contain the packet with its checksum field
    already set to zero.

    Args:
        data: Packet bytes with checksum field zeroed.

    Returns:
        16-bit checksum as an integer in the range 0x0000-0xFFFF.

    Raises:
        TypeError: If data is not bytes-like.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")

    data = bytes(data)

    # One's-complement checksum operates on 16-bit words.
    # Pad an odd number of bytes with one zero byte.
    if len(data) % 2:
        data += b"\x00"

    total = 0

    # Interpret every pair of bytes as a big-endian 16-bit word.
    for i in range(0, len(data), 2):
        word = (data[i] << 8) | data[i + 1]
        total += word

        # Fold carry back into the lower 16 bits.
        total = (total & 0xFFFF) + (total >> 16)

    # There can still be one final carry.
    while total >> 16:
        total = (total & 0xFFFF) + (total >> 16)

    # One's complement.
    return (~total) & 0xFFFF


def verify(data: bytes, expected: int) -> bool:
    """
    Verify a packet checksum.

    `data` must be the packet with the checksum field zeroed.

    Args:
        data: Packet bytes with checksum field zeroed.
        expected: Received 16-bit checksum.

    Returns:
        True if the calculated checksum equals expected,
        otherwise False.

    Raises:
        TypeError: If data is not bytes-like.
        ValueError: If expected is not a valid 16-bit value.
    """
    if not isinstance(expected, int):
        raise TypeError("expected checksum must be an integer")

    if not 0 <= expected <= 0xFFFF:
        raise ValueError("expected checksum must be a 16-bit value")

    calculated = checksum(data)

    return calculated == expected


def checksum_bytes(data: bytes) -> bytes:
    """
    Calculate the checksum and return it as two big-endian bytes.

    Useful when constructing a packet header with struct.pack().
    """
    value = checksum(data)
    return value.to_bytes(CHECKSUM_SIZE, byteorder="big")


def verify_packet(
    packet: bytes,
    checksum_offset: int,
) -> bool:
    """
    Verify a complete encoded packet.

    This helper extracts the checksum from `checksum_offset`,
    temporarily zeros that field, recalculates the checksum,
    and compares the result.

    Args:
        packet:
            Complete encoded packet.
        checksum_offset:
            Byte offset at which the 2-byte checksum field begins.

    Returns:
        True if valid, False if corrupted.

    Raises:
        TypeError: For invalid packet input.
        ValueError: For an invalid checksum offset or truncated packet.
    """
    if not isinstance(packet, (bytes, bytearray, memoryview)):
        raise TypeError("packet must be bytes-like")

    packet = bytes(packet)

    if checksum_offset < 0:
        raise ValueError("checksum_offset must be non-negative")

    if checksum_offset + CHECKSUM_SIZE > len(packet):
        raise ValueError("packet is truncated around checksum field")

    # Extract received checksum.
    expected = int.from_bytes(
        packet[checksum_offset:checksum_offset + CHECKSUM_SIZE],
        byteorder="big",
    )

    # Make a copy and zero the checksum field.
    packet_for_checksum = bytearray(packet)

    packet_for_checksum[
        checksum_offset:checksum_offset + CHECKSUM_SIZE
    ] = b"\x00\x00"

    return verify(packet_for_checksum, expected)