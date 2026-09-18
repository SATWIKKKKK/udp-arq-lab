"""Null transport implementation."""

import time
from udp_arq.transport.base import Transport, Address
from udp_arq.packet import encode, decode, Header, PacketError
from udp_arq.checksum import checksum, verify

class NullTransport(Transport):
    """A zero-retransmit passthrough transport."""
    
    def sendto(self, data: bytes, addr: Address) -> None:
        if len(data) > self.mss:
            raise ValueError(f"Payload size {len(data)} > mss {self.mss}")
            
        header = Header(seq=0, ack=0, payload_len=len(data))
        pkt_bytes = encode(header, data)
        csum = checksum(pkt_bytes)
        
        final_header = Header(seq=0, ack=0, payload_len=len(data), checksum=csum)
        final_pkt_bytes = encode(final_header, data)
        
        self.channel.sendto(final_pkt_bytes, addr)
        
    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]:
        start = time.monotonic()
        while True:
            rem = None
            if timeout is not None:
                elapsed = time.monotonic() - start
                rem = max(0.0, timeout - elapsed)
                if rem == 0.0:
                    raise TimeoutError()
                    
            try:
                data, addr = self.channel.recvfrom(timeout=rem)
            except TimeoutError:
                raise
                
            if not verify(data):
                continue
                
            try:
                header, payload = decode(data)
            except (PacketError, TypeError):
                continue
                
            return payload, addr
            
    def close(self) -> None:
        self.channel.close()
