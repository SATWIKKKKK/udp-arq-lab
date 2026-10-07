import re
import os

filepath = "udp_arq/transport/selective_repeat.py"
with open(filepath, "r") as f:
    content = f.read()

# 1. Imports and Class Name
content = content.replace("Go-Back-N ARQ transport (owner: Pratik).", "Selective Repeat ARQ transport (owner: Sagnik).")
content = content.replace("Go-Back-N provides reliable delivery using a sliding window.", "Selective Repeat provides reliable delivery using a sliding window.")
content = content.replace("GoBackNTransport", "SelectiveRepeatTransport")
content = content.replace("Go-Back-N", "Selective Repeat")
content = content.replace("GBN", "SR")
content = content.replace("gbn-sender", "sr-sender")

# Add TimerHeap import
import_stmt = "from udp_arq.transport.base import Address, Transport"
new_import_stmt = import_stmt + "\nfrom udp_arq.timer_heap import TimerHeap"
content = content.replace(import_stmt, new_import_stmt)

# 2. State initialization
state_init = """        # One timer deadline per peer.
        # It represents the oldest outstanding packet.
        self._timer_deadline: dict[Address, float | None] = {}

        # Number of timeout retransmissions for the current window.
        self._retries: dict[Address, int] = {}"""

new_state_init = """        # Timer heap for per-packet timeouts.
        self._timer_heap = TimerHeap()

        # Number of consecutive timeout events per peer (for max retries).
        self._retries: dict[Address, int] = {}
        
        # Track which packets have been retransmitted (for week 6 Karn's algorithm).
        self._retransmitted: dict[Address, set[int]] = {}
        
        # Total retransmissions (for stats).
        self.retransmissions = 0"""
content = content.replace(state_init, new_state_init)

# Receiver buffer
recv_state = """        # Next sequence number expected from each peer.
        self._expected: dict[Address, int] = {}"""
new_recv_state = recv_state + """

        # Out-of-order packet buffer per peer.
        self._buffer: dict[Address, dict[int, bytes]] = {}"""
content = content.replace(recv_state, new_recv_state)

# Drain outbound state init
drain_state = """                self._sent_items.setdefault(addr, {})
                self._timer_deadline.setdefault(addr, None)
                self._retries.setdefault(addr, 0)"""
new_drain_state = """                self._sent_items.setdefault(addr, {})
                self._retries.setdefault(addr, 0)
                self._retransmitted.setdefault(addr, set())
                self._buffer.setdefault(addr, {})"""
content = content.replace(drain_state, new_drain_state)

# 3. Timeout logic (_next_wait_time and _handle_timeouts)
next_wait_time = """    def _next_wait_time(self) -> float:
        \"\"\"Return seconds until the next timer expires, or a default.\"\"\"

        now = time.monotonic()
        wait = 1.0

        with self._lock:
            for deadline in self._timer_deadline.values():
                if deadline is not None:
                    remaining = deadline - now
                    if remaining < wait:
                        wait = remaining

        return max(0.001, wait)"""
new_next_wait_time = """    def _next_wait_time(self) -> float:
        \"\"\"Return seconds until the next timer expires, or a default.\"\"\"

        now = time.monotonic()
        wait = 1.0

        with self._lock:
            nearest = self._timer_heap.nearest()
            if nearest is not None:
                wait = nearest - now

        return max(0.001, min(wait, 1.0))"""
content = content.replace(next_wait_time, new_next_wait_time)

# 4. _handle_timeouts
handle_timeouts = """    def _handle_timeouts(self) -> None:
        \"\"\"Retransmit the complete outstanding window after timeout.\"\"\"

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
                    )"""

new_handle_timeouts = """    def _handle_timeouts(self) -> None:
        \"\"\"Retransmit specific unacknowledged packets after timeout.\"\"\"

        now = time.monotonic()
        
        with self._lock:
            due_keys = self._timer_heap.pop_due(now)
            
            # Group by address to handle MAX_RETRIES efficiently
            due_by_addr = {}
            for addr, seq in due_keys:
                if addr in self._outstanding and seq in self._outstanding[addr]:
                    due_by_addr.setdefault(addr, []).append(seq)
                    
            for addr, seqs in due_by_addr.items():
                self._retries[addr] = self._retries.get(addr, 0) + 1
                if self._retries[addr] > MAX_RETRIES:
                    self._fail_peer(
                        addr,
                        TimeoutError(
                            f"no ACK for peer {addr} "
                            f"after {MAX_RETRIES} "
                            f"retransmissions"
                        ),
                    )
                    continue
                    
                for seq in seqs:
                    # Double check it is still outstanding
                    packet = self._outstanding[addr].get(seq)
                    if packet is None:
                        continue
                        
                    self.retransmissions += 1
                    self._retransmitted.setdefault(addr, set()).add(seq)
                    
                    # Reschedule timer
                    self._timer_heap.schedule((addr, seq), now + self.timeout)
                    
                    try:
                        self.channel.sendto(packet, addr)
                    except Exception as exc:
                        self._fail_peer(addr, exc)
                        break"""
content = content.replace(handle_timeouts, new_handle_timeouts)


# 5. Pump logic
pump_timer_logic = """                    # Start ONE timer when the first packet enters
                    # an otherwise-empty window.
                    if (
                        self._base.get(addr) == seq
                        and self._timer_deadline.get(addr) is None
                    ):
                        self._timer_deadline[addr] = (
                            time.monotonic()
                            + self.timeout
                        )"""
new_pump_timer_logic = """                    # Start a timer for EACH packet sent.
                    self._timer_heap.schedule((addr, seq), time.monotonic() + self.timeout)"""
content = content.replace(pump_timer_logic, new_pump_timer_logic)
content = content.replace("                        self._timer_deadline[addr] = None", "                        self._timer_heap.cancel((addr, seq))")


# 6. ACK Handling
ack_logic = """            # --------------------------------------------------------
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
                self._timer_deadline[addr] = None"""

new_ack_logic = """            # --------------------------------------------------------
            # Individual ACK (Selective Repeat):
            #
            # ACK N means: packet (N-1) is acknowledged.
            # --------------------------------------------------------
            
            acked_seq = ack - 1
            
            outstanding = self._outstanding.get(addr, {})
            sent_items = self._sent_items.get(addr, {})
            
            if acked_seq in outstanding:
                outstanding.pop(acked_seq)
                item = sent_items.pop(acked_seq, None)
                if item is not None:
                    item["event"].set()
                    
                self._timer_heap.cancel((addr, acked_seq))
                
                # Advance base past all acknowledged packets
                while base < next_seq and base not in outstanding:
                    base += 1
                self._base[addr] = base
                
                # Progress resets retry count
                self._retries[addr] = 0"""
content = content.replace(ack_logic, new_ack_logic)

# Remove the stale ACK check in _handle_ack
stale_ack_check = """            # Stale ACK.
            if ack <= base:
                return"""
new_stale_ack_check = """            # Wait, in SR, an ACK for seq < base is valid but already processed. 
            if ack <= base:
                pass # Already advanced base past this"""
content = content.replace(stale_ack_check, new_stale_ack_check)


# 7. Receiver logic (_handle_incoming DATA path)
rx_logic = """        with self._lock:
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
                # Selective Repeat does NOT buffer it.
                #
                # Re-send the cumulative ACK for the current expected
                # sequence number.
                ack_for = expected

                deliver = False

        # ------------------------------------------------------------
        # Send cumulative ACK immediately.
        # ------------------------------------------------------------"""

new_rx_logic = """        with self._lock:
            expected = self._expected.get(addr, 0)
            buffer = self._buffer.setdefault(addr, {})

            if expected <= header.seq < expected + self.window_size:
                # Inside window
                buffer[header.seq] = payload
                ack_for = header.seq + 1
            elif header.seq < expected:
                # Duplicate, already delivered, still need to ACK it
                ack_for = header.seq + 1
            else:
                # Outside window (too far ahead)
                ack_for = expected

            # Deliver in-order packets
            deliver_queue = []
            while expected in buffer:
                deliver_queue.append(buffer.pop(expected))
                expected += 1
            
            self._expected[addr] = expected

        # ------------------------------------------------------------
        # Send individual ACK immediately.
        # ------------------------------------------------------------"""
content = content.replace(rx_logic, new_rx_logic)

rx_deliver = """        if deliver:
            self._incoming_data.put(
                (payload, addr)
            )"""
new_rx_deliver = """        for p in deliver_queue:
            self._incoming_data.put((p, addr))"""
content = content.replace(rx_deliver, new_rx_deliver)

with open(filepath, "w") as f:
    f.write(content)
