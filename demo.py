r"""Quick reliability demo for the viva.

Run:

    .\.venv\Scripts\python.exe demo.py

It sends the same 16 KiB file through a channel with 25% packet loss:

1. NullTransport - no ARQ, so lost packets stay lost.
2. Stop-and-Wait, Go-Back-N, Selective Repeat - ARQ recovers every chunk.

The SHA-256 comparison proves the received file is byte-for-byte identical
to the original.
"""

from __future__ import annotations

import os
import threading
import time

from udp_arq.channel import Channel
from udp_arq.file_layer import chunk, reassemble, sha256_hex
from udp_arq.transport.go_back_n import GoBackNTransport
from udp_arq.transport.null import NullTransport
from udp_arq.transport.selective_repeat import SelectiveRepeatTransport
from udp_arq.transport.stop_and_wait import StopAndWaitTransport

LOCALHOST = ("127.0.0.1", 0)
LOSS = 0.25
SEED = 7
TIMEOUT = 0.10
FILE_SIZE = 16 * 1024
MSS = 1024


def _sha(data: bytes) -> str:
    return sha256_hex(data)


def run_arq(name, transport_cls, kwargs, chunks, original_hash):
    sender = transport_cls(
        Channel(LOCALHOST, loss=LOSS, seed=SEED),
        timeout=TIMEOUT,
        **kwargs,
    )
    receiver = transport_cls(Channel(LOCALHOST), timeout=TIMEOUT, **kwargs)
    addr = receiver.channel.local_addr
    received: list[bytes] = []

    def receive_all() -> None:
        for _ in chunks:
            received.append(receiver.recvfrom(timeout=15.0)[0])

    thread = threading.Thread(target=receive_all)
    thread.start()
    started = time.monotonic()

    try:
        for piece in chunks:
            sender.sendto(piece, addr)

        flush = getattr(sender, "flush", None)
        if flush is not None:
            flush(timeout=60.0)

        thread.join(timeout=60.0)
        elapsed = time.monotonic() - started

        rebuilt = reassemble(received)
        ok = len(received) == len(chunks) and _sha(rebuilt) == original_hash
        resends = sender.channel.sent - len(chunks)

        return (
            name,
            len(chunks),
            sender.channel.dropped,
            sender.channel.sent,
            len(received),
            resends,
            "PASS" if ok else "FAIL",
            elapsed,
        )
    finally:
        sender.close()
        receiver.close()


def run_null(chunks, original_hash):
    sender = NullTransport(
        Channel(LOCALHOST, loss=LOSS, seed=SEED),
        timeout=TIMEOUT,
    )
    receiver = NullTransport(Channel(LOCALHOST), timeout=TIMEOUT)
    addr = receiver.channel.local_addr
    received: list[bytes] = []

    def receive_all() -> None:
        for _ in chunks:
            try:
                received.append(receiver.recvfrom(timeout=0.3)[0])
            except TimeoutError:
                break

    thread = threading.Thread(target=receive_all)
    thread.start()
    started = time.monotonic()

    try:
        for piece in chunks:
            sender.sendto(piece, addr)

        thread.join(timeout=2.0)
        elapsed = time.monotonic() - started

        ok = (
            len(received) == len(chunks)
            and _sha(reassemble(received)) == original_hash
        )

        return (
            "Null (no ARQ)",
            len(chunks),
            sender.channel.dropped,
            sender.channel.sent,
            len(received),
            0,
            "PASS" if ok else "FAIL",
            elapsed,
        )
    finally:
        sender.close()
        receiver.close()


def main() -> None:
    data = os.urandom(FILE_SIZE)
    chunks = chunk(data, mss=MSS)
    original_hash = _sha(data)

    print("=" * 78)
    print("UDP ARQ LAB - RELIABILITY DEMO")
    print("=" * 78)
    print(f"File size       : {len(data)} bytes ({len(chunks)} chunks)")
    print(f"Channel loss    : {LOSS:.0%}")
    print(f"Original SHA-256: {original_hash}")
    print()

    results = [run_null(chunks, original_hash)]
    results.append(
        run_arq(
            "Stop-and-Wait",
            StopAndWaitTransport,
            {},
            chunks,
            original_hash,
        )
    )
    results.append(
        run_arq(
            "Go-Back-N",
            GoBackNTransport,
            {"window_size": 4},
            chunks,
            original_hash,
        )
    )
    results.append(
        run_arq(
            "Selective Repeat",
            SelectiveRepeatTransport,
            {"window_size": 4},
            chunks,
            original_hash,
        )
    )

    header = (
        f"{'Protocol':18} {'Chunks':>6} {'Dropped':>7} {'DataPkts':>8} "
        f"{'Received':>8} {'Resends':>7} {'SHA-256':>7} {'Time':>6}"
    )
    print(header)
    print("-" * len(header))
    for row in results:
        name, total, dropped, sent, received, resends, status, elapsed = row
        print(
            f"{name:18} {total:>6} {dropped:>7} {sent:>8} "
            f"{received:>8} {resends:>7} {status:>7} {elapsed:>5.2f}s"
        )

    print()
    print("WHAT THIS SHOWS")
    print("-" * 78)
    print("NullTransport has no retransmission, so dropped packets stay lost")
    print("and the SHA-256 check fails.")
    print()
    print("All three ARQ protocols retransmit until every chunk is acknowledged.")
    print("They receive all chunks, report extra DataPkts as retransmissions,")
    print("and the SHA-256 matches the original: the file is intact despite loss.")


if __name__ == "__main__":
    main()
