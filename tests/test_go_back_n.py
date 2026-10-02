"""Tests for udp_arq.transport.go_back_n (owner: Pratik).

Run from the repo root:

    python -m pytest tests/test_go_back_n.py -q

The test setup mirrors test_stop_and_wait.py:
the sender's channel carries packet loss, while the receiver's
channel is loss-free so ACKs are not lost.
"""

import os
import threading

import pytest

from udp_arq.channel import Channel
from udp_arq.checksum import checksum
from udp_arq.file_layer import chunk, reassemble, sha256_hex
from udp_arq.packet import FLAG_DATA, Header, encode
from udp_arq.transport.go_back_n import GoBackNTransport


LOCALHOST = ("127.0.0.1", 0)


def make_pair(
    *,
    loss: float = 0.0,
    seed: int | None = None,
    timeout: float = 0.2,
    window_size: int = 4,
):
    """Create a sender/receiver GBN pair.

    Packet loss is applied only to the sender's channel.
    The receiver's ACK path is loss-free.
    """

    sender_ch = Channel(
        LOCALHOST,
        loss=loss,
        seed=seed,
    )

    receiver_ch = Channel(LOCALHOST)

    sender = GoBackNTransport(
        sender_ch,
        timeout=timeout,
        window_size=window_size,
    )

    receiver = GoBackNTransport(
        receiver_ch,
        timeout=timeout,
        window_size=window_size,
    )

    return sender, receiver


def test_single_datagram_round_trip() -> None:
    sender, receiver = make_pair()

    try:
        addr = receiver.channel.local_addr

        received: list[tuple[bytes, tuple]] = []

        t = threading.Thread(
            target=lambda: received.append(
                receiver.recvfrom(timeout=2.0)
            )
        )

        t.start()

        sender.sendto(b"hello", addr)

        t.join(timeout=3.0)

        assert received, "receiver.recvfrom never returned"

        data, from_addr = received[0]

        assert data == b"hello"
        assert from_addr == sender.channel.local_addr

    finally:
        sender.close()
        receiver.close()


def test_ordered_datagrams_survive_multiple() -> None:
    sender, receiver = make_pair()

    try:
        addr = receiver.channel.local_addr

        payloads = [
            f"packet-{i}".encode()
            for i in range(5)
        ]

        received: list[bytes] = []

        def receive_all() -> None:
            for _ in payloads:
                data, _ = receiver.recvfrom(timeout=5.0)
                received.append(data)

        t = threading.Thread(target=receive_all)
        t.start()

        for payload in payloads:
            sender.sendto(payload, addr)

        t.join(timeout=10.0)

        assert received == payloads

    finally:
        sender.close()
        receiver.close()


def test_out_of_order_packet_dropped_not_delivered() -> None:
    sender, receiver = make_pair()

    try:
        addr = receiver.channel.local_addr

        # Receiver initially expects seq=0.
        #
        # Send seq=5 instead. It is out of order and must be
        # discarded rather than delivered.
        draft = encode(
            Header(
                seq=5,
                ack=0,
                flags=FLAG_DATA,
                payload_len=5,
            ),
            b"wrong",
        )

        packet = encode(
            Header(
                seq=5,
                ack=0,
                flags=FLAG_DATA,
                payload_len=5,
                checksum=checksum(draft),
            ),
            b"wrong",
        )

        sender.channel.sendto(packet, addr)

        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.3)

        # Now send the correct seq=0 packet.
        draft = encode(
            Header(
                seq=0,
                ack=0,
                flags=FLAG_DATA,
                payload_len=5,
            ),
            b"right",
        )

        packet = encode(
            Header(
                seq=0,
                ack=0,
                flags=FLAG_DATA,
                payload_len=5,
                checksum=checksum(draft),
            ),
            b"right",
        )

        sender.channel.sendto(packet, addr)

        data, _ = receiver.recvfrom(timeout=2.0)

        assert data == b"right"

    finally:
        sender.close()
        receiver.close()


def test_oversized_payload_rejected() -> None:
    sender, receiver = make_pair()

    try:
        with pytest.raises(ValueError):
            sender.sendto(
                b"x" * (sender.mss + 1),
                receiver.channel.local_addr,
            )

    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("loss", [0.0, 0.10])
def test_file_transfer_sha256_matches(loss: float) -> None:
    """Week 3 file-transfer target with 0% and 10% packet loss."""

    data = os.urandom(64 * 1024)

    chunks = chunk(
        data,
        mss=1024,
    )

    sender, receiver = make_pair(
        loss=loss,
        seed=99,
        timeout=0.1,
        window_size=4,
    )

    try:
        addr = receiver.channel.local_addr

        received_chunks: list[bytes] = []

        def receive_all() -> None:
            for _ in chunks:
                payload, _ = receiver.recvfrom(timeout=10.0)
                received_chunks.append(payload)

        t = threading.Thread(target=receive_all)
        t.start()

        for piece in chunks:
            sender.sendto(piece, addr)

        t.join(timeout=60.0)

        assert len(received_chunks) == len(chunks)

        rebuilt = reassemble(received_chunks)

        assert sha256_hex(rebuilt) == sha256_hex(data)

    finally:
        sender.close()
        receiver.close()


def test_window_size_one() -> None:
    """window_size=1 should still behave correctly."""

    data = os.urandom(16 * 1024)

    chunks = chunk(
        data,
        mss=1024,
    )

    sender, receiver = make_pair(
        loss=0.0,
        timeout=0.1,
        window_size=1,
    )

    try:
        addr = receiver.channel.local_addr

        received_chunks: list[bytes] = []

        def receive_all() -> None:
            for _ in chunks:
                payload, _ = receiver.recvfrom(timeout=10.0)
                received_chunks.append(payload)

        t = threading.Thread(target=receive_all)
        t.start()

        for piece in chunks:
            sender.sendto(piece, addr)

        t.join(timeout=30.0)

        assert len(received_chunks) == len(chunks)

        rebuilt = reassemble(received_chunks)

        assert sha256_hex(rebuilt) == sha256_hex(data)

    finally:
        sender.close()
        receiver.close()
        