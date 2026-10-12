"""Cross-protocol correctness suite (owner: Pratik, shared with the team).

Every scenario runs against every ARQ transport so a protocol-specific
bug cannot hide. Selective Repeat is skipped until Sagnik's
``udp_arq.transport.selective_repeat`` lands on main; its row lights up
automatically afterwards.

Run: python -m pytest tests/test_correctness.py -q
"""

import os
import threading

import pytest

from udp_arq.channel import Channel
from udp_arq.checksum import checksum
from udp_arq.file_layer import chunk, reassemble, sha256_hex
from udp_arq.packet import FLAG_DATA, Header, encode
from udp_arq.transport.go_back_n import GoBackNTransport
from udp_arq.transport.stop_and_wait import StopAndWaitTransport

try:
    from udp_arq.transport.selective_repeat import SelectiveRepeatTransport
    SR_AVAILABLE = True
except ImportError:
    SelectiveRepeatTransport = None
    SR_AVAILABLE = False


LOCALHOST = ("127.0.0.1", 0)


def _sr_or_skip(channel, **kwargs):
    if not SR_AVAILABLE:
        pytest.skip("Selective Repeat not merged yet")
    return SelectiveRepeatTransport(channel, **kwargs)


PROTOCOLS = {
    "stopwait": StopAndWaitTransport,
    "gbn": GoBackNTransport,
    "sr": _sr_or_skip,
}


def make_pair(proto, *, loss=0.0, seed=None, timeout=0.2, **kwargs):
    """Sender's channel carries the loss; ACK path is loss-free."""
    sender = PROTOCOLS[proto](
        Channel(LOCALHOST, loss=loss, seed=seed), timeout=timeout, **kwargs
    )
    receiver = PROTOCOLS[proto](Channel(LOCALHOST), timeout=timeout, **kwargs)
    return sender, receiver


def flush_if_possible(sender) -> None:
    """GBN (and later SR) confirm delivery via flush(); Stop-and-Wait's
    sendto already blocks until ACKed."""
    flush = getattr(sender, "flush", None)
    if flush is not None:
        flush()


@pytest.mark.parametrize("proto", sorted(PROTOCOLS))
def test_single_datagram_round_trip(proto):
    sender, receiver = make_pair(proto)
    try:
        addr = receiver.channel.local_addr
        box: list[tuple[bytes, tuple]] = []
        t = threading.Thread(
            target=lambda: box.append(receiver.recvfrom(timeout=2.0))
        )
        t.start()
        sender.sendto(b"hello", addr)
        flush_if_possible(sender)
        t.join(timeout=3.0)
        assert box, "receiver.recvfrom never returned"
        data, from_addr = box[0]
        assert data == b"hello"
        assert from_addr == sender.channel.local_addr
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("proto", sorted(PROTOCOLS))
def test_ordered_datagrams(proto):
    payloads = [f"packet-{i}".encode() for i in range(6)]
    sender, receiver = make_pair(proto)
    try:
        addr = receiver.channel.local_addr
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in payloads:
                received.append(receiver.recvfrom(timeout=5.0)[0])

        t = threading.Thread(target=receive_all)
        t.start()
        for p in payloads:
            sender.sendto(p, addr)
        flush_if_possible(sender)
        t.join(timeout=10.0)
        assert received == payloads
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("proto", sorted(PROTOCOLS))
def test_duplicate_data_not_redelivered(proto):
    """A replayed DATA packet (retransmit after lost ACK) must be
    re-ACKed but never delivered twice."""
    sender, receiver = make_pair(proto)
    try:
        addr = receiver.channel.local_addr

        draft = encode(
            Header(seq=0, ack=0, flags=FLAG_DATA, payload_len=5), b"first"
        )
        packet = encode(
            Header(
                seq=0,
                ack=0,
                flags=FLAG_DATA,
                payload_len=5,
                checksum=checksum(draft),
            ),
            b"first",
        )

        received: list[tuple[bytes, tuple]] = []
        t = threading.Thread(
            target=lambda: received.append(receiver.recvfrom(timeout=2.0))
        )
        t.start()
        sender.channel.sendto(packet, addr)
        t.join(timeout=3.0)
        assert received and received[0][0] == b"first"

        # Same seq again: duplicate, must not be delivered a second time.
        sender.channel.sendto(packet, addr)
        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.3)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("proto", sorted(PROTOCOLS))
def test_oversized_payload_rejected(proto):
    sender, receiver = make_pair(proto)
    try:
        with pytest.raises(ValueError):
            sender.sendto(b"x" * (sender.mss + 1), receiver.channel.local_addr)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("proto", sorted(PROTOCOLS))
@pytest.mark.parametrize("loss", [0.0, 0.10])
def test_file_transfer_sha256_matches(proto, loss):
    data = os.urandom(32 * 1024)
    chunks_ = chunk(data, mss=1024)

    sender, receiver = make_pair(proto, loss=loss, seed=99, timeout=0.1)
    try:
        addr = receiver.channel.local_addr
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in chunks_:
                received.append(receiver.recvfrom(timeout=15.0)[0])

        t = threading.Thread(target=receive_all)
        t.start()
        for piece in chunks_:
            sender.sendto(piece, addr)
        flush_if_possible(sender)
        t.join(timeout=30.0)

        assert len(received) == len(chunks_)
        assert sha256_hex(reassemble(received)) == sha256_hex(data)
    finally:
        sender.close()
        receiver.close()


@pytest.mark.parametrize("proto", ["stopwait", "gbn"])
@pytest.mark.parametrize("loss", [0.0, 0.10])
def test_adaptive_rto_file_transfer(proto, loss):
    """Adaptive RTO (Jacobson/Karels + Karn) must not break correctness.
    SR joins this list once Sagnik wires the ``adaptive`` kwarg there."""
    data = os.urandom(32 * 1024)
    chunks_ = chunk(data, mss=1024)

    sender, receiver = make_pair(
        proto, loss=loss, seed=99, timeout=0.1, adaptive=True
    )
    try:
        addr = receiver.channel.local_addr
        received: list[bytes] = []

        def receive_all() -> None:
            for _ in chunks_:
                received.append(receiver.recvfrom(timeout=15.0)[0])

        t = threading.Thread(target=receive_all)
        t.start()
        for piece in chunks_:
            sender.sendto(piece, addr)
        flush_if_possible(sender)
        t.join(timeout=30.0)

        assert len(received) == len(chunks_)
        assert sha256_hex(reassemble(received)) == sha256_hex(data)
    finally:
        sender.close()
        receiver.close()