"""Tests for udp_arq.transport.selective_repeat (owner: Sagnik).

Run from the repo root:

    python -m pytest tests/test_selective_repeat.py -q
"""

import os
import threading
import time

import pytest

from udp_arq.channel import Channel
from udp_arq.checksum import checksum
from udp_arq.file_layer import chunk, reassemble, sha256_hex
from udp_arq.packet import FLAG_DATA, Header, encode
from udp_arq.transport.selective_repeat import SelectiveRepeatTransport

LOCALHOST = ("127.0.0.1", 0)


def make_pair(
    *,
    loss: float = 0.0,
    seed: int | None = None,
    timeout: float = 0.2,
    window_size: int = 4,
    receiver_loss: float = 0.0,
    receiver_seed: int | None = None,
):
    """Create a sender/receiver SR pair."""
    sender_ch = Channel(
        LOCALHOST,
        loss=loss,
        seed=seed,
    )
    receiver_ch = Channel(
        LOCALHOST,
        loss=receiver_loss,
        seed=receiver_seed,
    )

    sender = SelectiveRepeatTransport(
        sender_ch,
        timeout=timeout,
        window_size=window_size,
    )

    receiver = SelectiveRepeatTransport(
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
        sender.flush()
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
        payloads = [f"packet-{i}".encode() for i in range(5)]
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in payloads:
                data, _ = receiver.recvfrom(timeout=5.0)
                received.append(data)

        t = threading.Thread(target=receive_all)
        t.start()

        for payload in payloads:
            sender.sendto(payload, addr)

        sender.flush()
        t.join(timeout=10.0)

        assert received == payloads
    finally:
        sender.close()
        receiver.close()


def test_out_of_order_packets_buffered_then_delivered_in_order() -> None:
    sender, receiver = make_pair()

    try:
        addr = receiver.channel.local_addr
        payloads = [b"p0", b"p1", b"p2"]
        seqs = [2, 1, 0]

        for s in seqs:
            draft = encode(Header(seq=s, ack=0, flags=FLAG_DATA, payload_len=2), payloads[s])
            packet = encode(
                Header(seq=s, ack=0, flags=FLAG_DATA, payload_len=2, checksum=checksum(draft)),
                payloads[s]
            )
            sender.channel.sendto(packet, addr)

        received = []
        for _ in range(3):
            data, _ = receiver.recvfrom(timeout=2.0)
            received.append(data)

        assert received == payloads
    finally:
        sender.close()
        receiver.close()


def test_duplicate_data_not_redelivered() -> None:
    sender, receiver = make_pair()

    try:
        addr = receiver.channel.local_addr
        draft = encode(Header(seq=0, ack=0, flags=FLAG_DATA, payload_len=2), b"p0")
        packet = encode(
            Header(seq=0, ack=0, flags=FLAG_DATA, payload_len=2, checksum=checksum(draft)),
            b"p0"
        )
        
        sender.channel.sendto(packet, addr)
        sender.channel.sendto(packet, addr)

        data, _ = receiver.recvfrom(timeout=2.0)
        assert data == b"p0"

        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.5)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("loss", [0.0, 0.10])
def test_file_transfer_sha256_matches(loss: float) -> None:
    data = os.urandom(64 * 1024)
    chunks = chunk(data, mss=1024)
    sender, receiver = make_pair(loss=loss, seed=99, timeout=0.1, window_size=4)

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

        sender.flush(timeout=60.0)
        t.join(timeout=60.0)

        assert len(received_chunks) == len(chunks)
        rebuilt = reassemble(received_chunks)
        assert sha256_hex(rebuilt) == sha256_hex(data)
    finally:
        sender.close()
        receiver.close()


def test_window_size_one() -> None:
    data = os.urandom(16 * 1024)
    chunks = chunk(data, mss=1024)
    sender, receiver = make_pair(loss=0.0, timeout=0.1, window_size=1)

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

        sender.flush()
        t.join(timeout=30.0)

        assert len(received_chunks) == len(chunks)
        rebuilt = reassemble(received_chunks)
        assert sha256_hex(rebuilt) == sha256_hex(data)
    finally:
        sender.close()
        receiver.close()


def test_selective_retransmit_not_whole_window() -> None:
    # Deterministic via seeded ACK loss: sender channel clean, receiver channel loss=0.5
    # window 4, 4 packets. Some ACKs die -> timeout -> SR retransmits only unacked packets.
    sender, receiver = make_pair(
        loss=0.0,
        receiver_loss=0.5,
        receiver_seed=6,
        timeout=0.2,
        window_size=4
    )

    try:
        addr = receiver.channel.local_addr
        payloads = [f"packet-{i}".encode() for i in range(4)]
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in payloads:
                data, _ = receiver.recvfrom(timeout=5.0)
                received.append(data)

        t = threading.Thread(target=receive_all)
        t.start()

        for payload in payloads:
            sender.sendto(payload, addr)

        sender.flush(timeout=10.0)
        t.join(timeout=10.0)

        assert received == payloads
        # GBN would resend all 4 from base. SR should resend fewer.
        assert sender.retransmissions > 0
        assert sender.retransmissions < 4
    finally:
        sender.close()
        receiver.close()


def test_zero_loss_has_zero_retransmissions() -> None:
    sender, receiver = make_pair(loss=0.0)
    try:
        addr = receiver.channel.local_addr
        payloads = [f"packet-{i}".encode() for i in range(4)]
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in payloads:
                data, _ = receiver.recvfrom(timeout=5.0)
                received.append(data)

        t = threading.Thread(target=receive_all)
        t.start()

        for payload in payloads:
            sender.sendto(payload, addr)

        sender.flush(timeout=10.0)
        t.join(timeout=10.0)

        assert received == payloads
        assert sender.retransmissions == 0
    finally:
        sender.close()
        receiver.close()