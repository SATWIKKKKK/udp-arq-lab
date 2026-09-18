"""Tests for udp_arq.channel (owner: Ahana).

Run from the repo root: python -m pytest tests/test_channel.py -q
"""

import time

import pytest

from udp_arq.channel import MAX_DATAGRAM, Channel

LOCALHOST = ("127.0.0.1", 0)  # port 0: the OS picks a free port


def drop_pattern(seed: int, n: int = 1000, loss: float = 0.3) -> list[int]:
    """Indices of the packets the channel dropped out of ``n`` sends."""
    dropped = []
    with Channel(LOCALHOST, loss=loss, jitter=0.001, seed=seed) as ch:
        dest = ch.local_addr
        for i in range(n):
            before = ch.dropped
            ch.sendto(i.to_bytes(4, "big"), dest)
            if ch.dropped != before:
                dropped.append(i)
    return dropped


def test_same_seed_gives_identical_drop_sequence() -> None:
    first = drop_pattern(seed=42)
    second = drop_pattern(seed=42)
    assert first  # 30% loss over 1000 packets must drop something
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
    payloads = [f"packet-{i}".encode() for i in range(100)]
    with Channel(LOCALHOST, seed=1) as sender, Channel(LOCALHOST) as receiver:
        for p in payloads:
            sender.sendto(p, receiver.local_addr)

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
    with Channel(LOCALHOST, loss=1.0, seed=1) as sender, Channel(LOCALHOST) as receiver:
        for _ in range(20):
            sender.sendto(b"gone", receiver.local_addr)
        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.2)
        assert sender.dropped == 20
        assert sender.delivered == 0


def test_delay_is_applied() -> None:
    with Channel(LOCALHOST, delay=0.2) as sender, Channel(LOCALHOST) as receiver:
        start = time.monotonic()
        sender.sendto(b"late", receiver.local_addr)
        data, _ = receiver.recvfrom(timeout=2.0)
        elapsed = time.monotonic() - start
    assert data == b"late"
    assert elapsed >= 0.19


def test_jitter_keeps_delay_in_range() -> None:
    with Channel(LOCALHOST, delay=0.1, jitter=0.05, seed=3) as sender, Channel(
        LOCALHOST
    ) as receiver:
        start = time.monotonic()
        for i in range(20):
            sender.sendto(bytes([i]), receiver.local_addr)
        arrivals = []
        for _ in range(20):
            receiver.recvfrom(timeout=2.0)
            arrivals.append(time.monotonic() - start)
    assert len(arrivals) == 20
    assert min(arrivals) >= 0.045  # delay - jitter, minus a little slack


def test_recvfrom_times_out() -> None:
    with Channel(LOCALHOST) as ch:
        start = time.monotonic()
        with pytest.raises(TimeoutError):
            ch.recvfrom(timeout=0.1)
        assert time.monotonic() - start >= 0.09


def test_close_flushes_pending_packets() -> None:
    with Channel(LOCALHOST) as receiver:
        sender = Channel(LOCALHOST, delay=0.2)
        sender.sendto(b"last", receiver.local_addr)
        sender.close()  # must wait for the delayed packet, not drop it
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
        ch.sendto(b"x", ("127.0.0.1", 9))


def test_oversized_datagram_rejected() -> None:
    with Channel(LOCALHOST) as ch:
        with pytest.raises(ValueError):
            ch.sendto(b"x" * (MAX_DATAGRAM + 1), ch.local_addr)


def test_non_bytes_rejected() -> None:
    with Channel(LOCALHOST) as ch:
        with pytest.raises(TypeError):
            ch.sendto("text", ch.local_addr)


def test_negative_timeout_rejected() -> None:
    with Channel(LOCALHOST) as ch:
        with pytest.raises(ValueError):
            ch.recvfrom(timeout=-1)


@pytest.mark.parametrize(
    "kwargs",
    [{"loss": -0.1}, {"loss": 1.5}, {"delay": -1}, {"jitter": -1}],
)
def test_invalid_parameters_rejected(kwargs: dict) -> None:
    with pytest.raises(ValueError):
        Channel(LOCALHOST, **kwargs)