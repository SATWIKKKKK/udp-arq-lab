"""Stop-and-Wait ARQ transport (owner: Satwik).

Classic alternating-bit protocol: one outstanding DATA packet at a time,
sequence numbers 0/1, cumulative ACK per the frozen header ("ack = next
expected sequence number").

``sendto`` blocks, retransmitting on each per-attempt timeout, until the
matching ACK arrives. ``recvfrom`` re-ACKs and drops duplicate DATA packets
(the sender's previous ACK was lost, not the data) instead of handing the
same payload to the caller twice.

Each side needs its own send bit and its own expected-receive bit *per
peer address*, tracked independently, because a single ``StopAndWaitTransport``
instance can be used to both send and receive (e.g. a receiver ACKing back
to a sender that is itself retransmitting).
"""

from __future__ import annotations

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

# Safety cap on retransmissions so a bug (or 100% loss) fails loudly with a
# TimeoutError instead of hanging the caller forever. Not part of the frozen
# interface -- just a guard.
MAX_RETRIES = 1000


def _encode_with_checksum(header: Header, payload: bytes) -> bytes:
    """Encode ``header``+``payload`` with the checksum field filled in.

    Two-pass: encode once with checksum 0 to get the exact on-wire bytes,
    compute the checksum over that, then encode again with the real value.
    """
    draft = encode(header, payload)
    csum = checksum(draft)
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


class StopAndWaitTransport(Transport):
    """Reliable single-datagram delivery, one packet in flight at a time."""

    def __init__(self, channel, *, mss: int = DEFAULT_MSS, timeout: float = 0.5):
        super().__init__(channel, mss=mss, timeout=timeout)
        self._send_bit: dict[Address, int] = {}
        self._recv_bit: dict[Address, int] = {}

    def sendto(self, data: bytes, addr: Address) -> None:
        """Reliably deliver one datagram, retransmitting until ACKed."""
        if len(data) > self.mss:
            raise ValueError(f"payload size {len(data)} > mss {self.mss}")

        seq = self._send_bit.get(addr, 0)
        packet = _encode_with_checksum(
            Header(seq=seq, ack=0, flags=FLAG_DATA, payload_len=len(data)), data
        )

        for _ in range(MAX_RETRIES):
            self.channel.sendto(packet, addr)
            deadline = time.monotonic() + self.timeout

            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break  # per-attempt timeout: retransmit
                try:
                    raw, from_addr = self.channel.recvfrom(timeout=remaining)
                except TimeoutError:
                    break
                if from_addr != addr:
                    continue
                if not verify(raw):
                    continue
                try:
                    header, _ = decode(raw)
                except PacketError:
                    continue
                if not (header.flags & FLAG_ACK):
                    continue
                if header.ack == (seq ^ 1):
                    self._send_bit[addr] = seq ^ 1
                    return
                # Stale ACK for a previous packet: keep waiting for ours.
        raise TimeoutError(
            f"no ACK for seq {seq} to {addr} after {MAX_RETRIES} retransmissions"
        )

    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]:
        """Next in-order datagram + sender, ACKing (and re-ACKing) as needed."""
        start = time.monotonic()
        while True:
            remaining = None
            if timeout is not None:
                remaining = timeout - (time.monotonic() - start)
                if remaining <= 0:
                    raise TimeoutError(f"no datagram within {timeout} s")

            raw, addr = self.channel.recvfrom(timeout=remaining)

            if not verify(raw):
                continue
            try:
                header, payload = decode(raw)
            except PacketError:
                continue
            if not (header.flags & FLAG_DATA):
                continue  # stray ACK addressed to us; ignore

            expected = self._recv_bit.get(addr, 0)
            if header.seq == expected:
                self._recv_bit[addr] = expected ^ 1
                ack_for = self._recv_bit[addr]
                new_data = True
            else:
                # Duplicate: our previous ACK was lost, not the data.
                # Re-ACK what we've already accepted; don't redeliver it.
                ack_for = expected
                new_data = False

            ack_packet = _encode_with_checksum(
                Header(seq=0, ack=ack_for, flags=FLAG_ACK, payload_len=0), b""
            )
            self.channel.sendto(ack_packet, addr)

            if new_data:
                return payload, addr

    def close(self) -> None:
        self.channel.close()
