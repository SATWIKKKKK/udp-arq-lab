"""Channel emulator v1 for the UDP ARQ lab (owner: Ahana).

The emulator is a *shim*: ``Channel`` wraps one real UDP socket and applies
impairment on the send path only.

    sendto(data, addr)
        -> loss decision (seeded RNG)       -> dropped, counted, gone
        -> deliver_at = now + delay + jitter -> pushed onto a heap
    background thread
        -> pops the heap when deliver_at is reached -> real socket.sendto

``recvfrom`` reads straight from the real socket; the receive path adds no
impairment.

Determinism: every packet consumes exactly two draws from the single
``random.Random`` (one for loss, one for jitter), whether it is dropped or
not. The same seed therefore gives the same drop sequence on every run.
"""

from __future__ import annotations

import heapq
import itertools
import random
import select
import socket
import threading
import time

Address = tuple[str, int]

# Largest UDP payload that fits in one IPv4 datagram (65535 - 8-byte UDP
# header - 20-byte IPv4 header).
MAX_DATAGRAM = 65507


class Channel:
    """Lossy, delaying UDP endpoint. See the README "Channel emulator" section."""

    def __init__(
        self,
        local_addr: Address,
        *,
        loss: float = 0.0,
        delay: float = 0.0,
        jitter: float = 0.0,
        seed: int | None = None,
    ) -> None:
        if not 0.0 <= loss <= 1.0:
            raise ValueError(f"loss must be in [0, 1], got {loss}")
        if delay < 0:
            raise ValueError(f"delay must be >= 0, got {delay}")
        if jitter < 0:
            raise ValueError(f"jitter must be >= 0, got {jitter}")

        self.loss = loss
        self.delay = delay
        self.jitter = jitter
        self._rng = random.Random(seed)

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # On Windows, an ICMP "port unreachable" makes the *next* recvfrom
        # fail with WSAECONNRESET. A lossy channel must not do that.
        if hasattr(socket, "SIO_UDP_CONNRESET"):
            self._sock.ioctl(socket.SIO_UDP_CONNRESET, False)
        self._sock.bind(local_addr)

        # Heap entries: (deliver_at, tiebreak, data, addr). The tiebreak
        # keeps equal deadlines in send order and stops heapq comparing bytes.
        self._heap: list[tuple[float, int, bytes, Address]] = []
        self._tiebreak = itertools.count()
        self._cond = threading.Condition()
        self._closed = False

        self._sent = 0
        self._dropped = 0
        self._delivered = 0

        self._worker = threading.Thread(
            target=self._deliver_loop, name="channel-delivery", daemon=True
        )
        self._worker.start()

    # ------------------------------------------------------------------ API

    def sendto(self, data: bytes, addr: Address) -> None:
        """Queue ``data`` for ``addr``, subject to loss, delay, and jitter.

        Returns immediately; delivery happens on the background thread.
        Raises ``ValueError`` if ``data`` is larger than one UDP datagram and
        ``OSError`` if the channel is closed.
        """
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError(f"data must be bytes-like, got {type(data).__name__}")
        data = bytes(data)
        if len(data) > MAX_DATAGRAM:
            raise ValueError(f"datagram too large: {len(data)} > {MAX_DATAGRAM}")

        with self._cond:
            if self._closed:
                raise OSError("channel is closed")
            self._sent += 1

            # Always draw both numbers so the RNG stream stays aligned.
            dropped = self._rng.random() < self.loss
            offset = self._rng.uniform(-self.jitter, self.jitter)

            if dropped:
                self._dropped += 1
                return

            now = time.monotonic()
            deliver_at = max(now, now + self.delay + offset)
            heapq.heappush(
                self._heap, (deliver_at, next(self._tiebreak), data, addr)
            )
            self._cond.notify()

    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]:
        """Return the next datagram and its sender.

        ``timeout=None`` blocks forever. Raises ``TimeoutError`` on expiry.
        """
        if timeout is not None and timeout < 0:
            raise ValueError(f"timeout must be >= 0 or None, got {timeout}")
        # select() instead of settimeout(): the socket timeout would also
        # apply to sendto on the delivery thread.
        readable, _, _ = select.select([self._sock], [], [], timeout)
        if not readable:
            raise TimeoutError(f"no datagram within {timeout} s")
        data, addr = self._sock.recvfrom(MAX_DATAGRAM)
        return data, addr

    def close(self) -> None:
        """Deliver every packet still queued, then close the socket.

        Pending packets keep their scheduled times, so this can block for up
        to ``delay + jitter`` seconds. Calling ``close`` twice is harmless.
        """
        with self._cond:
            if self._closed:
                return
            self._closed = True
            self._cond.notify()
        self._worker.join()
        self._sock.close()

    @property
    def local_addr(self) -> Address:
        """The bound ``(host, port)``; useful after binding to port 0."""
        return self._sock.getsockname()

    @property
    def sent(self) -> int:
        """Number of ``sendto`` calls accepted."""
        return self._sent

    @property
    def dropped(self) -> int:
        """Number of packets discarded by the loss model."""
        return self._dropped

    @property
    def delivered(self) -> int:
        """Number of packets handed to the real socket."""
        return self._delivered

    def __enter__(self) -> Channel:
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.close()

    # ------------------------------------------------------------ internals

    def _deliver_loop(self) -> None:
        with self._cond:
            while True:
                if not self._heap:
                    if self._closed:
                        return
                    self._cond.wait()
                    continue

                deliver_at, _, data, addr = self._heap[0]
                wait = deliver_at - time.monotonic()
                if wait > 0:
                    self._cond.wait(wait)
                    continue

                heapq.heappop(self._heap)
                try:
                    self._sock.sendto(data, addr)
                except OSError:
                    # Unreachable destination etc.: behaves like loss on the
                    # wire, but is not counted as delivered.
                    continue
                self._delivered += 1
