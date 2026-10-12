"""Adaptive RTO estimation: Jacobson/Karels + Karn (owner: Pratik).

Pure, thread-free state machine. Transports own the actual timer
deadlines; this class only turns RTT samples and timeout events into an
RTO value.

RFC 6298 (simplified for the lab):
    first sample : SRTT = RTT, RTTVAR = RTT / 2
    later        : RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - RTT|
                   SRTT   = (1 - alpha) * SRTT + alpha * RTT
    RTO          = SRTT + max(G, K * RTTVAR), clamped to [min_rto, max_rto]
    on_timeout   : RTO *= 2 (exponential backoff)

Karn's algorithm lives in the transport: RTT samples from retransmitted
packets are never passed to sample(); the transport calls on_timeout()
instead and resumes sampling only when a fresh (non-retransmitted)
packet is acknowledged.
"""

from __future__ import annotations

ALPHA = 0.125  # RFC 6298 SRTT smoothing factor
BETA = 0.25    # RFC 6298 RTTVAR smoothing factor
K = 4          # RFC 6298 RTTVAR multiplier


class RTOEstimator:
    """Jacobson/Karels RTO estimator with exponential backoff."""

    def __init__(
        self,
        initial_rto: float = 0.5,
        min_rto: float = 0.1,
        max_rto: float = 5.0,
        clock_granularity: float = 0.05,
    ) -> None:
        if not (0 < min_rto <= initial_rto <= max_rto):
            raise ValueError("require 0 < min_rto <= initial_rto <= max_rto")
        self.min_rto = min_rto
        self.max_rto = max_rto
        self.g = clock_granularity
        self.srtt: float | None = None
        self.rttvar: float | None = None
        self.rto = initial_rto

    def sample(self, rtt: float) -> float:
        """Feed one RTT measurement from a non-retransmitted packet."""
        if rtt <= 0:
            raise ValueError("rtt must be positive")

        if self.srtt is None:
            self.srtt = rtt
            self.rttvar = rtt / 2.0
        else:
            self.rttvar = (1 - BETA) * self.rttvar + BETA * abs(self.srtt - rtt)
            self.srtt = (1 - ALPHA) * self.srtt + ALPHA * rtt

        self.rto = max(
            self.min_rto,
            min(self.max_rto, self.srtt + max(self.g, K * self.rttvar)),
        )
        return self.rto

    def on_timeout(self) -> float:
        """Exponential backoff after a timeout; returns the new RTO."""
        self.rto = min(self.max_rto, self.rto * 2.0)
        return self.rto