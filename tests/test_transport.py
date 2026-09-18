import os
import pytest
import tempfile
from pathlib import Path
from udp_arq.transport.null import NullTransport
from udp_arq.channel import Channel
from udp_arq.file_layer import read_file_chunks, reassemble_file, files_match

LOCALHOST = ("127.0.0.1", 0)

def test_roundtrip_success():
    with Channel(LOCALHOST) as sender_ch, Channel(LOCALHOST) as receiver_ch:
        sender = NullTransport(sender_ch)
        receiver = NullTransport(receiver_ch)
        
        sender.sendto(b"hello world", receiver_ch.local_addr)
        data, addr = receiver.recvfrom(timeout=1.0)
        
        assert data == b"hello world"
        assert addr == sender_ch.local_addr

def test_oversized_payload_raises_value_error():
    with Channel(LOCALHOST) as ch:
        transport = NullTransport(ch, mss=10)
        with pytest.raises(ValueError):
            transport.sendto(b"12345678901", ch.local_addr)

def test_corruption_handling():
    with Channel(LOCALHOST) as sender_ch, Channel(LOCALHOST) as receiver_ch:
        sender = NullTransport(sender_ch)
        receiver = NullTransport(receiver_ch)
        
        sender.sendto(b"good data", receiver_ch.local_addr)
        
        # Corrupt the packet manually in the channel's local buffer or just send a corrupted packet
        # Actually, let's send a corrupted packet directly using the channel
        sender_ch.sendto(b"X" * 20, receiver_ch.local_addr) 
        
        # It should ignore the corrupted packet and return "good data"
        data, addr = receiver.recvfrom(timeout=1.0)
        assert data == b"good data"
        
        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.1)

def test_truncation_handling():
    with Channel(LOCALHOST) as sender_ch, Channel(LOCALHOST) as receiver_ch:
        sender = NullTransport(sender_ch)
        receiver = NullTransport(receiver_ch)
        
        # Send a truncated packet (e.g. 5 bytes) directly
        sender_ch.sendto(b"12345", receiver_ch.local_addr)
        
        with pytest.raises(TimeoutError):
            receiver.recvfrom(timeout=0.1)

def test_file_transfer_harness(tmp_path):
    # Create a 1MB test file
    source_file = tmp_path / "source.bin"
    dest_file = tmp_path / "dest.bin"
    
    with open(source_file, "wb") as f:
        f.write(os.urandom(1024 * 1024))
        
    with Channel(LOCALHOST) as sender_ch, Channel(LOCALHOST) as receiver_ch:
        sender = NullTransport(sender_ch)
        receiver = NullTransport(receiver_ch)
        
        chunks = list(read_file_chunks(source_file, mss=sender.mss))
        received_chunks = []
        
        import threading
        def receive_all():
            for _ in range(len(chunks)):
                data, _ = receiver.recvfrom(timeout=5.0)
                received_chunks.append(data)
                
        t = threading.Thread(target=receive_all)
        t.start()
        
        for chunk in chunks:
            sender.sendto(chunk, receiver_ch.local_addr)
            # small sleep to ensure we don't overwhelm the socket buffer
            import time
            time.sleep(0.001)
            
        t.join()
        
        reassemble_file(received_chunks, dest_file)
        assert files_match(source_file, dest_file)
