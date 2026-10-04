"""Tests for udp_arq.channel (owner: Ahana).

Run from the repo root:

    python -m pytest tests/test_channel.py -q
"""

import os
import threading
import time

import pytest

from udp_arq.channel import MAX_DATAGRAM, Channel
from udp_arq.checksum import verify
from udp_arq.file_layer import chunk, reassemble, sha256_hex
from udp_arq.transport.stop_and_wait import StopAndWaitTransport

LOCALHOST = ("127.0.0.1", 0)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def drop_pattern(
    seed: int,
    n: int = 1000,
    loss: float = 0.3,
) -> list[int]:
    """Indices of packets the channel dropped out of ``n`` sends."""
    dropped = []

    with Channel(
        LOCALHOST,
        loss=loss,
        jitter=0.001,
        seed=seed,
    ) as ch:
        dest = ch.local_addr

        for i in range(n):
            before = ch.dropped
            ch.sendto(i.to_bytes(4, "big"), dest)

            if ch.dropped != before:
                dropped.append(i)

    return dropped


def impairment_sequence(
    seed: int,
    n: int = 100,
) -> tuple[list[bool], int, list[bytes]]:
    """Run all five impairments and record the resulting sequence.

    Returns:
        dropped_flags:
            Whether each sendto call was dropped.

        delivered:
            Total number of datagrams handed to the real socket.

        received:
            Payload bytes in actual arrival order.
    """
    dropped_flags: list[bool] = []
    received: list[bytes] = []

    sender = Channel(
        LOCALHOST,
        loss=0.30,
        jitter=0.001,
        duplication=0.50,
        reorder=0.50,
        corruption=0.50,
        seed=seed,
    )
    receiver = Channel(LOCALHOST)

    try:
        dest = receiver.local_addr

        for i in range(n):
            payload = i.to_bytes(8, "big")

            before = sender.dropped
            sender.sendto(payload, dest)

            dropped_flags.append(sender.dropped != before)

        # close() flushes all packets still scheduled in the sender heap.
        sender.close()

        # At this point delivered is final.
        expected = sender.delivered

        for _ in range(expected):
            data, _ = receiver.recvfrom(timeout=3.0)
            received.append(data)

        return dropped_flags, sender.delivered, received

    finally:
        # sender may already be closed.
        sender.close()
        receiver.close()


# ---------------------------------------------------------------------------
# Existing v1 tests
# ---------------------------------------------------------------------------


def test_same_seed_gives_identical_drop_sequence() -> None:
    first = drop_pattern(seed=42)
    second = drop_pattern(seed=42)

    assert first
    assert first == second


def test_different_seeds_give_different_drop_sequences() -> None:
    assert drop_pattern(seed=1) != drop_pattern(seed=2)


def test_measured_loss_within_one_percent() -> None:
    n = 10_000

    with Channel(LOCALHOST, loss=0.1, seed=7) as ch:
        dest = ch.local_addr

        for _ in range(n):
            ch.sendto(b"x", dest)

        assert ch.sent == n
        assert 0.09 <= ch.dropped / n <= 0.11


def test_zero_loss_delivers_everything_intact() -> None:
    payloads = [
        f"packet-{i}".encode()
        for i in range(100)
    ]

    with Channel(LOCALHOST, seed=1) as sender, Channel(
        LOCALHOST
    ) as receiver:

        for payload in payloads:
            sender.sendto(payload, receiver.local_addr)

        received = []

        for _ in payloads:
            data, addr = receiver.recvfrom(timeout=2.0)
            received.append(data)

            assert addr == sender.local_addr

        assert sorted(received) == sorted(payloads)
        assert sender.sent == 100
        assert sender.dropped == 0
        assert sender.delivered == 100


def test_full_loss_delivers_nothing() -> None:
    with Channel(
        LOCALHOST,
        loss=1.0,
        seed=1,
    ) as sender, Channel(LOCALHOST) as receiver:

        for _ in range(20):
            sender.sendto(
                b"gone",
                receiver.local_addr,
            )

        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.2)

        assert sender.dropped == 20
        assert sender.delivered == 0


def test_delay_is_applied() -> None:
    with Channel(
        LOCALHOST,
        delay=0.2,
    ) as sender, Channel(LOCALHOST) as receiver:

        start = time.monotonic()

        sender.sendto(
            b"late",
            receiver.local_addr,
        )

        data, _ = receiver.recvfrom(timeout=2.0)

        elapsed = time.monotonic() - start

    assert data == b"late"
    assert elapsed >= 0.19


def test_jitter_keeps_delay_in_range() -> None:
    with Channel(
        LOCALHOST,
        delay=0.1,
        jitter=0.05,
        seed=3,
    ) as sender, Channel(LOCALHOST) as receiver:

        start = time.monotonic()

        for i in range(20):
            sender.sendto(
                bytes([i]),
                receiver.local_addr,
            )

        arrivals = []

        for _ in range(20):
            receiver.recvfrom(timeout=2.0)
            arrivals.append(time.monotonic() - start)

    assert len(arrivals) == 20
    assert min(arrivals) >= 0.045


def test_recvfrom_times_out() -> None:
    with Channel(LOCALHOST) as ch:
        start = time.monotonic()

        with pytest.raises(TimeoutError):
            ch.recvfrom(timeout=0.1)

        assert time.monotonic() - start >= 0.09


def test_close_flushes_pending_packets() -> None:
    with Channel(LOCALHOST) as receiver:
        sender = Channel(
            LOCALHOST,
            delay=0.2,
        )

        sender.sendto(
            b"last",
            receiver.local_addr,
        )

        sender.close()

        data, _ = receiver.recvfrom(timeout=1.0)

        assert data == b"last"
        assert sender.delivered == 1


def test_close_is_idempotent() -> None:
    ch = Channel(LOCALHOST)

    ch.close()
    ch.close()


def test_sendto_after_close_raises() -> None:
    ch = Channel(LOCALHOST)

    ch.close()

    with pytest.raises(OSError):
        ch.sendto(
            b"x",
            ("127.0.0.1", 9),
        )


def test_oversized_datagram_rejected() -> None:
    with Channel(LOCALHOST) as ch:
        with pytest.raises(ValueError):
            ch.sendto(
                b"x" * (MAX_DATAGRAM + 1),
                ch.local_addr,
            )


def test_non_bytes_rejected() -> None:
    with Channel(LOCALHOST) as ch:
        with pytest.raises(TypeError):
            ch.sendto(
                "text",
                ch.local_addr,
            )


def test_negative_timeout_rejected() -> None:
    with Channel(LOCALHOST) as ch:
        with pytest.raises(ValueError):
            ch.recvfrom(timeout=-1)


# ---------------------------------------------------------------------------
# New v2 tests
# ---------------------------------------------------------------------------


def test_same_seed_gives_identical_impairment_sequence() -> None:
    """Same seed must produce the same five-draw impairment sequence."""

    first = impairment_sequence(seed=42)
    second = impairment_sequence(seed=42)

    assert first == second


def test_duplication_doubles_delivery() -> None:
    """duplication=1.0 must deliver every packet exactly twice."""

    payloads = [
        f"packet-{i}".encode()
        for i in range(20)
    ]

    with Channel(
        LOCALHOST,
        duplication=1.0,
        seed=42,
    ) as sender, Channel(LOCALHOST) as receiver:

        for payload in payloads:
            sender.sendto(
                payload,
                receiver.local_addr,
            )

        # Flush the sender queue before checking delivered.
        sender.close()

        assert sender.sent == len(payloads)
        assert sender.dropped == 0
        assert sender.delivered == 2 * len(payloads)

        received = []

        for _ in range(2 * len(payloads)):
            data, _ = receiver.recvfrom(timeout=2.0)
            received.append(data)

        assert sorted(received) == sorted(payloads + payloads)


def test_reorder_can_overtake() -> None:
    """Large reorder values should allow later packets to arrive first."""

    payloads = [
        i.to_bytes(4, "big")
        for i in range(50)
    ]

    with Channel(
        LOCALHOST,
        reorder=1.0,
        seed=123,
    ) as sender, Channel(LOCALHOST) as receiver:

        for payload in payloads:
            sender.sendto(
                payload,
                receiver.local_addr,
            )

        sender.close()

        assert sender.delivered == len(payloads)

        received = []

        for _ in payloads:
            data, _ = receiver.recvfrom(timeout=3.0)
            received.append(data)

        # Every packet must arrive.
        assert sorted(received) == sorted(payloads)

        # But the seeded reorder impairment should change the order.
        assert received != payloads


def test_corruption_flips_payload_bits() -> None:
    """corruption=1.0 must modify every non-empty payload."""

    original = b"this payload must be corrupted"

    with Channel(
        LOCALHOST,
        corruption=1.0,
        seed=42,
    ) as sender, Channel(LOCALHOST) as receiver:

        sender.sendto(
            original,
            receiver.local_addr,
        )

        sender.close()

        corrupted, _ = receiver.recvfrom(timeout=2.0)

        assert corrupted != original


def test_corruption_caught_by_checksum() -> None:
    """Corrupting a checksummed packet must make checksum verification fail."""

    original_payload = b"checksum should catch this"

    # Build a valid DATA packet exactly as Stop-and-Wait does.
    from udp_arq.packet import (
        FLAG_DATA,
        Header,
        encode,
    )
    from udp_arq.checksum import checksum

    draft = encode(
        Header(
            seq=0,
            ack=0,
            flags=FLAG_DATA,
            payload_len=len(original_payload),
        ),
        original_payload,
    )

    packet = encode(
        Header(
            seq=0,
            ack=0,
            flags=FLAG_DATA,
            payload_len=len(original_payload),
            checksum=checksum(draft),
        ),
        original_payload,
    )

    assert verify(packet)

    with Channel(
        LOCALHOST,
        corruption=1.0,
        seed=42,
    ) as sender, Channel(LOCALHOST) as receiver:

        sender.sendto(
            packet,
            receiver.local_addr,
        )

        sender.close()

        corrupted_packet, _ = receiver.recvfrom(timeout=2.0)

        assert corrupted_packet != packet
        assert not verify(corrupted_packet)


def test_corruption_caught_end_to_end() -> None:
    """Stop-and-Wait must retransmit corrupted packets until a clean copy
    arrives, producing an identical file SHA-256.
    """

    data = os.urandom(64 * 1024)
    chunks = chunk(data, mss=1024)

    # Corruption only affects DATA packets leaving the sender.
    #
    # ACKs travel over the receiver's clean channel.
    sender_channel = Channel(
        LOCALHOST,
        corruption=0.5,
        seed=99,
    )

    receiver_channel = Channel(LOCALHOST)

    sender = StopAndWaitTransport(
        sender_channel,
        timeout=0.1,
    )

    receiver = StopAndWaitTransport(
        receiver_channel,
        timeout=0.1,
    )

    received_chunks: list[bytes] = []

    try:
        addr = receiver.channel.local_addr

        def receive_all() -> None:
            for _ in chunks:
                payload, _ = receiver.recvfrom(timeout=30.0)
                received_chunks.append(payload)

        thread = threading.Thread(
            target=receive_all,
            daemon=True,
        )

        thread.start()

        for piece in chunks:
            sender.sendto(piece, addr)

        thread.join(timeout=60.0)

        assert not thread.is_alive()
        assert len(received_chunks) == len(chunks)

        rebuilt = reassemble(received_chunks)

        assert sha256_hex(rebuilt) == sha256_hex(data)

    finally:
        sender.close()
        receiver.close()


# ---------------------------------------------------------------------------
# Parameter validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kwargs",
    [
        {"loss": -0.1},
        {"loss": 1.5},
        {"delay": -1},
        {"jitter": -1},
        {"duplication": -0.1},
        {"duplication": 1.1},
        {"reorder": -0.1},
        {"reorder": 1.1},
        {"corruption": -0.1},
        {"corruption": 1.1},
    ],
)
def test_invalid_parameters_rejected(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        Channel(LOCALHOST, **kwargs)