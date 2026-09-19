"""Tests for udp_arq.transport.stop_and_wait (owner: Satwik).

Run from the repo root: python -m pytest tests/test_stop_and_wait.py -q

Stop-and-Wait needs both ends live at once: sendto() blocks until it gets
an ACK, and that ACK is only produced by the peer's recvfrom(). So every
test here drives the receiver on a background thread while sendto() runs
on the main thread, the same pattern the file-transfer harness uses.
"""

import os
import threading

import pytest

from udp_arq.channel import Channel
from udp_arq.checksum import checksum
from udp_arq.file_layer import chunk, reassemble, sha256_hex
from udp_arq.packet import FLAG_DATA, Header, encode
from udp_arq.transport.stop_and_wait import StopAndWaitTransport

LOCALHOST = ("127.0.0.1", 0)


def make_pair(*, loss: float = 0.0, seed: int | None = None, timeout: float = 0.2):
    """A connected (sender_transport, receiver_transport) pair.

    Loss is applied on the sender's channel, matching how a real link drops
    packets leaving the sender; the receiver's ACK path is loss-free so
    tests exercise one direction of loss at a time, same as the README's
    Stop-and-Wait target (0% / 10% loss).
    """
    sender_ch = Channel(LOCALHOST, loss=loss, seed=seed)
    receiver_ch = Channel(LOCALHOST)
    sender = StopAndWaitTransport(sender_ch, timeout=timeout)
    receiver = StopAndWaitTransport(receiver_ch, timeout=timeout)
    return sender, receiver


def test_single_datagram_round_trip() -> None:
    sender, receiver = make_pair()
    try:
        addr = receiver.channel.local_addr
        received: list[tuple[bytes, tuple]] = []
        t = threading.Thread(target=lambda: received.append(receiver.recvfrom(timeout=2.0)))
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


def test_alternating_bit_survives_multiple_datagrams() -> None:
    sender, receiver = make_pair()
    try:
        addr = receiver.channel.local_addr
        payloads = [f"packet-{i}".encode() for i in range(5)]
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in payloads:
                data, _ = receiver.recvfrom(timeout=3.0)
                received.append(data)

        t = threading.Thread(target=receive_all)
        t.start()
        for p in payloads:
            sender.sendto(p, addr)
        t.join(timeout=5.0)

        assert received == payloads
    finally:
        sender.close()
        receiver.close()


def test_oversized_payload_rejected() -> None:
    sender, receiver = make_pair()
    try:
        with pytest.raises(ValueError):
            sender.sendto(b"x" * (sender.mss + 1), receiver.channel.local_addr)
    finally:
        sender.close()
        receiver.close()


def test_delivers_despite_loss() -> None:
    sender, receiver = make_pair(loss=0.5, seed=11, timeout=0.05)
    try:
        addr = receiver.channel.local_addr
        payload = b"retransmit me"
        received: list[tuple[bytes, tuple]] = []
        t = threading.Thread(target=lambda: received.append(receiver.recvfrom(timeout=10.0)))
        t.start()
        sender.sendto(payload, addr)
        t.join(timeout=15.0)

        assert received
        assert received[0][0] == payload
    finally:
        sender.close()
        receiver.close()


def test_duplicate_data_not_redelivered() -> None:
    """A duplicate DATA packet (as if the sender retransmitted after its own
    ACK was lost) must be re-ACKed but not handed to the caller twice."""
    sender, receiver = make_pair()
    try:
        addr = receiver.channel.local_addr

        draft = encode(Header(seq=0, ack=0, flags=FLAG_DATA, payload_len=5), b"first")
        packet = encode(
            Header(seq=0, ack=0, flags=FLAG_DATA, payload_len=5, checksum=checksum(draft)),
            b"first",
        )

        received: list[tuple[bytes, tuple]] = []
        t = threading.Thread(target=lambda: received.append(receiver.recvfrom(timeout=2.0)))
        t.start()
        sender.channel.sendto(packet, addr)
        t.join(timeout=3.0)
        assert received and received[0][0] == b"first"

        # Replay the same seq=0 packet: it's a duplicate, not new data.
        sender.channel.sendto(packet, addr)
        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.3)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("loss", [0.0, 0.10])
def test_file_transfer_sha256_matches(loss: float) -> None:
    """Week 3 target: Stop-and-Wait over the emulator, 0% then 10% loss,
    SHA-256 matching on both ends."""
    data = os.urandom(64 * 1024)  # a few dozen MSS-sized chunks
    chunks = chunk(data, mss=1024)

    sender, receiver = make_pair(loss=loss, seed=99, timeout=0.1)
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
