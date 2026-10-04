"""Go-Back-N ARQ transport (owner: Pratik).

Go-Back-N provides reliable delivery using a sliding window.

Multiple DATA packets may be outstanding at once. ACKs are cumulative:
an ACK value represents the next sequence number expected by the receiver.

sendto() places data into an internal outbound queue and returns
immediately; flush() waits until every datagram handed over so far has
been cumulatively acknowledged. This keeps the sliding window full when
the application produces datagrams sequentially (a blocking sendto would
cap the pipeline depth at one packet, making GBN degenerate into
Stop-and-Wait).

A background thread is responsible for:
    - moving queued packets into the transmission window,
    - sending/retransmitting DATA packets,
    - receiving and processing ACK packets,
    - receiving DATA packets,
    - checking DATA sequence numbers,
    - immediately sending cumulative ACKs,
    - placing only in-order DATA payloads into the receive queue.

The public recvfrom() consumes already-validated in-order DATA packets
from that receive queue.

This design ensures that the background thread is the sole reader of
the underlying UDP socket. ACK generation does not depend on the
application calling recvfrom().
"""

from __future__ import annotations

import queue
import threading
import time

from udp_arq.checksum import checksum, verify
from udp_arq.packet import (
    DEFAULT_MSS,
    FLAG_ACK,
    FLAG_DATA,
    Header,
    PacketError,
    decode,
    encode,
)
from udp_arq.transport.base import Address, Transport


# Safety cap so 100% loss eventually fails instead of hanging forever.
MAX_RETRIES = 1000


def _encode_with_checksum(header: Header, payload: bytes) -> bytes:
    """Encode header + payload with the checksum field filled in."""

    # First encode with checksum=0.
    draft = encode(header, payload)

    # Calculate checksum over the exact wire representation.
    csum = checksum(draft)

    # Encode again with the real checksum.
    return encode(
        Header(
            seq=header.seq,
            ack=header.ack,
            flags=header.flags,
            version=header.version,
            payload_len=header.payload_len,
            checksum=csum,
        ),
        payload,
    )


class GoBackNTransport(Transport):
    """Reliable sliding-window transport using Go-Back-N ARQ."""

    def __init__(
        self,
        channel,
        *,
        mss: int = DEFAULT_MSS,
        timeout: float = 0.5,
        window_size: int = 4,
    ):
        super().__init__(
            channel,
            mss=mss,
            timeout=timeout,
        )

        if window_size <= 0:
            raise ValueError("window_size must be greater than zero")

        self.window_size = window_size

        # ============================================================
        # Sender state
        # ============================================================

        # Oldest unacknowledged sequence number per peer.
        self._base: dict[Address, int] = {}

        # Next sequence number to assign per peer.
        self._next_seq: dict[Address, int] = {}

        # Encoded packets currently outstanding in the GBN window.
        #
        # Example:
        #
        # {
        #     addr: {
        #         0: b"...",
        #         1: b"...",
        #         2: b"...",
        #     }
        # }
        self._outstanding: dict[Address, dict[int, bytes]] = {}

        # Original sendto() items corresponding to outstanding packets.
        # Needed so cumulative ACKs can wake the correct Event.
        self._sent_items: dict[Address, dict[int, dict]] = {}

        # Packets waiting to enter the transmission window.
        self._pending: dict[Address, dict[int, dict]] = {}

        # One timer deadline per peer.
        # It represents the oldest outstanding packet.
        self._timer_deadline: dict[Address, float | None] = {}

        # Number of timeout retransmissions for the current window.
        self._retries: dict[Address, int] = {}

        # ============================================================
        # Receiver state
        # ============================================================

        # Next sequence number expected from each peer.
        self._expected: dict[Address, int] = {}

        # In-order DATA payloads waiting for the application.
        #
        # The background socket-reader thread puts payloads here after
        # sequence validation and ACK generation.
        #
        # recvfrom() consumes from this queue.
        self._incoming_data: queue.Queue[
            tuple[bytes, Address]
        ] = queue.Queue()

        # ============================================================
        # Synchronization
        # ============================================================

        self._lock = threading.RLock()
        self._stop_event = threading.Event()

        # Application-level outbound queue.
        self._outbound: queue.Queue = queue.Queue()

        # Background sender/receiver thread.
        #
        # This is the ONLY thread that calls channel.recvfrom().
        self._sender_thread = threading.Thread(
            target=self._sender_loop,
            name="gbn-sender",
            daemon=True,
        )

        self._sender_thread.start()

    # ================================================================
    # Public sender API
    # ================================================================

    def sendto(
        self,
        data: bytes,
        addr: Address,
    ) -> None:
        """Reliably deliver one datagram using Go-Back-N."""

        if len(data) > self.mss:
            raise ValueError(
                f"payload size {len(data)} > mss {self.mss}"
            )

        if self._stop_event.is_set():
            raise RuntimeError("transport is closed")

        with self._lock:
            # Assign sequence number immediately.
            seq = self._next_seq.get(addr, 0)
            self._next_seq[addr] = seq + 1

            # Each sendto() call owns an Event.
            #
            # The ACK handler sets this Event once the sequence number
            # is cumulatively acknowledged.
            item = {
                "addr": addr,
                "seq": seq,
                "data": bytes(data),
                "event": threading.Event(),
                "error": None,
            }

            self._outbound.put(item)

        # Return without waiting: the background thread sends the packet
        # when window space is available. Callers that need delivery
        # confirmation (or error propagation) call flush().

    def flush(
        self,
        addr: Address | None = None,
        timeout: float = 60.0,
    ) -> None:
        """Block until every datagram handed to sendto() so far is ACKed.

        ``addr=None`` waits for all peers. Raises the first per-packet
        error (e.g. TimeoutError after MAX_RETRIES, or RuntimeError if
        the transport was closed) and ``TimeoutError`` if ``timeout``
        seconds pass with datagrams still unacknowledged.
        """

        deadline = time.monotonic() + timeout

        while True:
            with self._lock:
                if addr is not None:
                    addrs = [addr]
                else:
                    addrs = sorted(
                        set(self._pending)
                        | set(self._sent_items)
                    )

                items: list[dict] = []

                for a in addrs:
                    items.extend(
                        self._pending.get(a, {}).values()
                    )
                    items.extend(
                        self._sent_items.get(a, {}).values()
                    )

                if addr is None and not self._outbound.empty():
                    items.extend(
                        list(self._outbound.queue)
                    )

            errors = [
                item["error"]
                for item in items
                if item["error"] is not None
            ]

            if errors:
                raise errors[0]

            if not items:
                return

            remaining = deadline - time.monotonic()

            if remaining <= 0:
                raise TimeoutError(
                    "flush timed out: "
                    "datagrams still unacknowledged"
                )

            # Hop briefly: wait on one representative event, then
            # re-snapshot so items ACKed in the meantime disappear.
            items[0]["event"].wait(
                min(0.05, remaining)
            )

    # ================================================================
    # Public receiver API
    # ================================================================

    def recvfrom(
        self,
        timeout: float | None = None,
    ) -> tuple[bytes, Address]:
        """Return the next in-order DATA packet.

        The background socket-reader thread has already:
            - verified the checksum,
            - decoded the packet,
            - checked its sequence number,
            - sent the appropriate cumulative ACK.

        Therefore recvfrom() only needs to consume the payload queue.
        """

        try:
            payload, addr = self._incoming_data.get(
                timeout=timeout
            )

        except queue.Empty:
            raise TimeoutError(
                f"no datagram within {timeout} s"
            )

        return payload, addr

    # ================================================================
    # Background thread
    # ================================================================

    def _sender_loop(self) -> None:
        """Handle sending, ACKs, DATA reception, and retransmission."""

        while not self._stop_event.is_set():

            # --------------------------------------------------------
            # Move application requests into pending state.
            # --------------------------------------------------------

            self._drain_outbound()

            # --------------------------------------------------------
            # Fill available transmission windows.
            # --------------------------------------------------------

            self._pump_windows()

            # --------------------------------------------------------
            # Check timers.
            # --------------------------------------------------------

            self._handle_timeouts()

            if self._stop_event.is_set():
                break

            # --------------------------------------------------------
            # Wait for an incoming packet.
            # --------------------------------------------------------

            wait_time = self._next_wait_time()

            try:
                raw, addr = self.channel.recvfrom(
                    timeout=wait_time
                )

            except TimeoutError:
                continue

            except OSError:
                if self._stop_event.is_set():
                    break

                continue

            # --------------------------------------------------------
            # Determine whether this is an ACK or DATA packet.
            # --------------------------------------------------------

            self._handle_incoming(raw, addr)

    # ================================================================
    # Incoming packet handling
    # ================================================================

    def _handle_incoming(
        self,
        raw: bytes,
        addr: Address,
    ) -> None:
        """Handle one verified packet received by the background thread."""

        # ------------------------------------------------------------
        # Verify checksum.
        # ------------------------------------------------------------

        if not verify(raw):
            return

        # ------------------------------------------------------------
        # Decode packet.
        # ------------------------------------------------------------

        try:
            header, payload = decode(raw)

        except PacketError:
            return

        # ------------------------------------------------------------
        # ACK packets belong to the sender.
        # ------------------------------------------------------------

        if header.flags & FLAG_ACK:
            self._handle_ack(raw, addr)
            return

        # ------------------------------------------------------------
        # Ignore anything that isn't DATA.
        # ------------------------------------------------------------

        if not (header.flags & FLAG_DATA):
            return

        # ------------------------------------------------------------
        # Process DATA immediately.
        #
        # This is deliberately done in the background socket-reader
        # thread so ACK generation never depends on recvfrom().
        # ------------------------------------------------------------

        with self._lock:
            expected = self._expected.get(addr, 0)

            if header.seq == expected:
                # Correct in-order packet.
                self._expected[addr] = expected + 1

                # ACK means:
                # "this is the next sequence number I expect."
                ack_for = expected + 1

                deliver = True

            else:
                # Out-of-order or duplicate packet.
                #
                # Go-Back-N does NOT buffer it.
                #
                # Re-send the cumulative ACK for the current expected
                # sequence number.
                ack_for = expected

                deliver = False

        # ------------------------------------------------------------
        # Send cumulative ACK immediately.
        # ------------------------------------------------------------

        ack_packet = _encode_with_checksum(
            Header(
                seq=0,
                ack=ack_for,
                flags=FLAG_ACK,
                payload_len=0,
            ),
            b"",
        )

        try:
            self.channel.sendto(
                ack_packet,
                addr,
            )

        except Exception:
            return

        # ------------------------------------------------------------
        # Deliver only an in-order packet.
        # ------------------------------------------------------------

        if deliver:
            self._incoming_data.put(
                (payload, addr)
            )

    # ================================================================
    # Move outbound queue -> pending
    # ================================================================

    def _drain_outbound(self) -> None:
        """Move queued sendto() requests into pending state."""

        while True:
            try:
                item = self._outbound.get_nowait()

            except queue.Empty:
                return

            addr = item["addr"]
            seq = item["seq"]

            with self._lock:
                self._pending.setdefault(addr, {})
                self._pending[addr][seq] = item

                self._base.setdefault(addr, seq)
                self._outstanding.setdefault(addr, {})
                self._sent_items.setdefault(addr, {})
                self._timer_deadline.setdefault(addr, None)
                self._retries.setdefault(addr, 0)

    # ================================================================
    # Window pump
    # ================================================================

    def _pump_windows(self) -> None:
        """Send queued packets while there is room in each window."""

        with self._lock:
            addresses = list(self._pending.keys())

        for addr in addresses:

            while True:

                with self._lock:
                    base = self._base.get(addr, 0)

                    pending = self._pending.get(addr)

                    if not pending:
                        break

                    # Always send the oldest pending sequence first.
                    seq = min(pending)

                    # Window is full when the oldest waiting packet lies
                    # outside [base, base + window_size). Basing this on
                    # _next_seq (the assignment counter) is wrong once
                    # several sendto() calls are queued up.
                    if seq >= base + self.window_size:
                        break

                    item = pending.pop(seq)

                    # Build DATA packet.
                    packet = _encode_with_checksum(
                        Header(
                            seq=seq,
                            ack=0,
                            flags=FLAG_DATA,
                            payload_len=len(item["data"]),
                        ),
                        item["data"],
                    )

                    # Store encoded packet for retransmission.
                    self._outstanding.setdefault(
                        addr,
                        {}
                    )

                    self._outstanding[addr][seq] = packet

                    # Store original sendto() item.
                    self._sent_items.setdefault(
                        addr,
                        {}
                    )

                    self._sent_items[addr][seq] = item

                    # Start ONE timer when the first packet enters
                    # an otherwise-empty window.
                    if (
                        self._base.get(addr) == seq
                        and self._timer_deadline.get(addr) is None
                    ):
                        self._timer_deadline[addr] = (
                            time.monotonic()
                            + self.timeout
                        )

                # Do channel I/O outside the lock.
                try:
                    self.channel.sendto(
                        packet,
                        addr,
                    )

                except Exception as exc:
                    with self._lock:

                        self._outstanding.get(
                            addr,
                            {},
                        ).pop(seq, None)

                        self._sent_items.get(
                            addr,
                            {},
                        ).pop(seq, None)

                        item["error"] = exc
                        item["event"].set()

                        if not self._outstanding.get(addr):
                            self._timer_deadline[addr] = None

    # ================================================================
    # ACK handling
    # ================================================================

    def _handle_ack(
        self,
        raw: bytes,
        addr: Address,
    ) -> None:
        """Process one verified cumulative ACK."""

        try:
            header, _ = decode(raw)

        except PacketError:
            return

        if not (header.flags & FLAG_ACK):
            return

        with self._lock:

            # We don't have sender state for this peer.
            if addr not in self._base:
                return

            base = self._base[addr]

            next_seq = self._next_seq.get(
                addr,
                base,
            )

            ack = header.ack

            # Stale ACK.
            if ack <= base:
                return

            # ACK cannot go beyond packets assigned by sendto().
            if ack > next_seq:
                return

            # --------------------------------------------------------
            # Cumulative ACK:
            #
            # ACK 5 means:
            #     0,1,2,3,4 are acknowledged
            #     5 is the next expected sequence number
            # --------------------------------------------------------

            self._base[addr] = ack

            outstanding = self._outstanding.get(
                addr,
                {},
            )

            sent_items = self._sent_items.get(
                addr,
                {},
            )

            # Everything below ACK is acknowledged.
            acknowledged = [
                seq
                for seq in list(outstanding)
                if seq < ack
            ]

            for seq in acknowledged:

                outstanding.pop(
                    seq,
                    None,
                )

                item = sent_items.pop(
                    seq,
                    None,
                )

                if item is not None:
                    item["event"].set()

            # ACK progress resets retry count.
            self._retries[addr] = 0

            # --------------------------------------------------------
            # Timer handling.
            # --------------------------------------------------------

            if self._base[addr] < next_seq:

                # There are still outstanding packets.
                self._timer_deadline[addr] = (
                    time.monotonic()
                    + self.timeout
                )

            else:

                # Everything has been acknowledged.
                self._timer_deadline[addr] = None

    # ================================================================
    # Timeout / retransmission
    # ================================================================

    def _handle_timeouts(self) -> None:
        """Retransmit the complete outstanding window after timeout."""

        now = time.monotonic()

        with self._lock:
            addresses = list(
                self._timer_deadline.keys()
            )

        for addr in addresses:

            with self._lock:
                deadline = self._timer_deadline.get(addr)

                if deadline is None:
                    continue

                if now < deadline:
                    continue

                outstanding = dict(
                    self._outstanding.get(
                        addr,
                        {},
                    )
                )

                if not outstanding:
                    self._timer_deadline[addr] = None
                    continue

                # One timeout = one retry.
                self._retries[addr] = (
                    self._retries.get(addr, 0)
                    + 1
                )

                retries = self._retries[addr]

                if retries > MAX_RETRIES:

                    self._fail_peer(
                        addr,
                        TimeoutError(
                            f"no ACK for peer {addr} "
                            f"after {MAX_RETRIES} "
                            f"retransmissions"
                        ),
                    )

                    continue

            # --------------------------------------------------------
            # Go-Back-N:
            #
            # Retransmit ALL outstanding packets.
            # --------------------------------------------------------

            for seq in sorted(outstanding):

                packet = outstanding[seq]

                try:
                    self.channel.sendto(
                        packet,
                        addr,
                    )

                except Exception as exc:
                    self._fail_peer(
                        addr,
                        exc,
                    )

                    break

            # Restart timer for the same oldest outstanding packet.
            with self._lock:
                if self._outstanding.get(addr):

                    self._timer_deadline[addr] = (
                        time.monotonic()
                        + self.timeout
                    )

    # ================================================================
    # Timer calculation
    # ================================================================

    def _next_wait_time(self) -> float:
        """Return a short wait time for the background loop."""

        now = time.monotonic()

        # Check frequently so newly queued packets are handled promptly.
        wait = 0.05

        with self._lock:
            deadlines = [
                deadline
                for deadline in self._timer_deadline.values()
                if deadline is not None
            ]

        if deadlines:
            nearest = min(deadlines)

            wait = max(
                0.0,
                min(
                    wait,
                    nearest - now,
                ),
            )

        return wait

    # ================================================================
    # Failure handling
    # ================================================================

    def _fail_peer(
        self,
        addr: Address,
        error: Exception,
    ) -> None:
        """Fail all sendto() operations waiting on a peer."""

        with self._lock:

            # Fail transmitted packets.
            sent = self._sent_items.get(
                addr,
                {},
            )

            for item in sent.values():
                item["error"] = error
                item["event"].set()

            sent.clear()

            # Fail packets still waiting to enter the window.
            pending = self._pending.get(
                addr,
                {},
            )

            for item in pending.values():
                item["error"] = error
                item["event"].set()

            pending.clear()

            self._outstanding.get(
                addr,
                {},
            ).clear()

            self._timer_deadline[addr] = None

    # ================================================================
    # Shutdown
    # ================================================================

    def close(self) -> None:
        """Close the transport and stop the background thread."""

        if self._stop_event.is_set():
            return

        self._stop_event.set()

        error = RuntimeError(
            "transport is closed"
        )

        with self._lock:

            # Wake sendto() calls waiting for ACKs.
            for sent in self._sent_items.values():
                for item in sent.values():
                    item["error"] = error
                    item["event"].set()

            # Wake sendto() calls still pending.
            for pending in self._pending.values():
                for item in pending.values():
                    item["error"] = error
                    item["event"].set()

            # Also wake any requests still sitting in the outbound
            # queue.
            while True:
                try:
                    item = self._outbound.get_nowait()

                except queue.Empty:
                    break

                item["error"] = error
                item["event"].set()

        if self._sender_thread.is_alive():
            self._sender_thread.join(
                timeout=1.0
            )

        self.channel.close()