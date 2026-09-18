import socket
import random
import time

MAX_DATAGRAM = 65535

class Channel:
    def __init__(self, local_addr, *, loss=0.0, delay=0.0, jitter=0.0, seed=None):
        if loss < 0 or loss > 1 or delay < 0 or jitter < 0:
            raise ValueError()
        self.loss = loss
        self.delay = delay
        self.jitter = jitter
        self.rng = random.Random(seed)
        self.sent = 0
        self.dropped = 0
        self.delivered = 0
        self._closed = False
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(local_addr)
        self.local_addr = self.sock.getsockname()
        self.sock.setblocking(False)
        self._q = []
        
    def sendto(self, data, addr):
        if self._closed:
            raise OSError()
        if not isinstance(data, (bytes, bytearray, memoryview)):
            raise TypeError()
        data_bytes = bytes(data)
        if len(data_bytes) > MAX_DATAGRAM:
            raise ValueError()
            
        self.sent += 1
        if self.loss > 0 and self.rng.random() < self.loss:
            self.dropped += 1
            return
            
        deliver_at = time.monotonic() + self.delay
        if self.jitter > 0:
            deliver_at += self.rng.uniform(-self.jitter, self.jitter)
            
        self._q.append((deliver_at, data_bytes, addr))
        self._pump()
        
    def _pump(self):
        now = time.monotonic()
        ready = [item for item in self._q if item[0] <= now]
        for item in ready:
            self.sock.sendto(item[1], item[2])
            self._q.remove(item)

    def recvfrom(self, timeout=None):
        if timeout is not None and timeout < 0:
            raise ValueError()
            
        start = time.monotonic()
        while True:
            self._pump()
            try:
                data, addr = self.sock.recvfrom(MAX_DATAGRAM)
                self.delivered += 1
                return data, addr
            except BlockingIOError:
                pass
                
            if timeout is not None:
                if time.monotonic() - start >= timeout:
                    raise TimeoutError()
            time.sleep(0.001)

    def close(self):
        if self._closed: return
        self._closed = True
        # Send remaining packets
        for item in self._q:
            time.sleep(max(0, item[0] - time.monotonic()))
            self.sock.sendto(item[1], item[2])
        self.sock.close()
        
    def __enter__(self): return self
    def __exit__(self, *a): self.close()
