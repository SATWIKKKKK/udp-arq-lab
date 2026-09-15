"""Round-trip and garbage-input tests for udp_arq.packet (owner: Satwik).

Run from the repo root: python -m pytest tests/test_packet.py -q
"""

import os

import pytest

from udp_arq.packet import (
    FLAG_ACK,
    FLAG_DATA,
    HEADER_SIZE,
    MAX_PAYLOAD,
    PROTOCOL_VERSION,
    Header,
    PacketError,
    decode,
    encode,
)


def make_packet(
    payload: bytes,
    seq: int = 7,
    ack: int = 3,
    flags: int = FLAG_DATA,
    checksum: int = 0,
) -> bytes:
    return encode(
        Header(
            seq=seq,
            ack=ack,
            flags=flags,
            payload_len=len(payload),
            checksum=checksum,
        ),
        payload,
    )


@pytest.mark.parametrize(
    "payload",
    [
        b"",  # empty
        b"hello, udp-arq",  # small
        bytes(range(256)) * 4,  # 1 KiB, near default MSS
        os.urandom(4096),  # random
        os.urandom(MAX_PAYLOAD),  # max payload
    ],
    ids=["empty", "small", "1KiB", "random", "max"],
)
def test_round_trip(payload: bytes) -> None:
    header, decoded_payload = decode(make_packet(payload))
    assert decoded_payload == payload
    assert header.payload_len == len(payload)


def test_header_fields_survive_round_trip() -> None:
    header = Header(
        seq=0xDEADBEEF,
        ack=0x12345678,
        flags=FLAG_DATA | FLAG_ACK,
        payload_len=4,
        checksum=0xBEEF,
    )
    decoded, payload = decode(encode(header, b"data"))
    assert decoded == header
    assert payload == b"data"


def test_header_size_is_16() -> None:
    assert HEADER_SIZE == 16


def test_empty_payload_has_payload_len_zero() -> None:
    header, payload = decode(make_packet(b""))
    assert payload == b""
    assert header.payload_len == 0


def test_truncated_header_raises() -> None:
    packet = make_packet(b"x" * 100)
    for n in range(HEADER_SIZE):
        with pytest.raises(PacketError):
            decode(packet[:n])


def test_truncated_payload_raises() -> None:
    packet = make_packet(b"x" * 100)
    with pytest.raises(PacketError):
        decode(packet[:-1])


def test_bad_magic_raises() -> None:
    packet = bytearray(make_packet(b"data"))
    packet[14] = 0xFF
    packet[15] = 0xFF
    with pytest.raises(PacketError, match="magic"):
        decode(packet)


def test_bad_version_raises() -> None:
    packet = bytearray(make_packet(b"data"))
    packet[11] = PROTOCOL_VERSION + 1
    with pytest.raises(PacketError, match="version"):
        decode(packet)


def test_unknown_flag_bits_raise() -> None:
    packet = bytearray(make_packet(b"data"))
    packet[10] |= 0x80
    with pytest.raises(PacketError, match="flag"):
        decode(packet)


def test_payload_len_mismatch_raises() -> None:
    packet = bytearray(make_packet(b"data"))
    packet[12:14] = b"\x00\x05"  # claim 5 bytes, only 4 present
    with pytest.raises(PacketError, match="payload_len"):
        decode(packet)


def test_encode_rejects_wrong_payload_len() -> None:
    header = Header(payload_len=10)
    with pytest.raises(PacketError):
        encode(header, b"data")


def test_random_garbage_raises_cleanly() -> None:
    with pytest.raises(PacketError):
        decode(b"\x00" * 64)


def test_non_bytes_input_raises_type_error() -> None:
    with pytest.raises(TypeError):
        decode("not bytes")


def test_memoryview_and_bytearray_accepted() -> None:
    packet = make_packet(b"data")
    assert decode(memoryview(packet))[1] == b"data"
    assert decode(bytearray(packet))[1] == b"data"
