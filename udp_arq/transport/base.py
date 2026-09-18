"""Transport base interface (owner: Sagnik)."""

from __future__ import annotations
from abc import ABC, abstractmethod

# Defined according to the frozen API in README
Address = tuple[str, int]
DEFAULT_MSS = 1024

class Transport(ABC):
    def __init__(self, channel, *, mss: int = DEFAULT_MSS, timeout: float = 0.5):
        self.channel = channel
        self.mss = mss
        self.timeout = timeout

    @abstractmethod
    def sendto(self, data: bytes, addr: Address) -> None:
        """Reliably deliver one datagram (len(data) <= mss).
        
        Blocks and retransmits until acknowledged.
        Raises ValueError if len(data) > mss.
        """
        pass

    @abstractmethod
    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]:
        """Next received datagram + sender.
        
        Raises TimeoutError on expiry.
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Close the transport."""
        pass
