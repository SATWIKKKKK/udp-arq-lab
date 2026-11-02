# udp-arq-lab

Reliable data transfer over an unreliable UDP channel.

This project implements a complete ARQ stack in Python: a frozen 16-byte wire
format, an RFC 1071 checksum, a file layer with SHA-256 verification, a
channel emulator that injects real network impairments, and three reliable
transports — **Stop-and-Wait**, **Go-Back-N**, and **Selective Repeat** —
with adaptive RTO estimation.

**Status:** 123 tests passing. All three ARQ protocols transfer files at
**0% and 10% loss** with matching SHA-256 hashes on both ends.

---

## Contents

- [What the project does](#what-the-project-does)
- [Quick start](#quick-start)
- [Reliability demo](#reliability-demo)
- [Architecture](#architecture)
- [Wire format](#wire-format-frozen)
- [Checksum](#checksum)
- [Channel emulator](#channel-emulator)
- [Transport interface](#transport-interface)
- [Protocols](#protocols)
- [Adaptive RTO](#adaptive-rto)
- [File layer](#file-layer)
- [Repository layout](#repository-layout)
- [Testing](#testing)
- [Design decisions](#design-decisions)
- [Limitations and next steps](#limitations-and-next-steps)
- [Team and ownership](#team-and-ownership)
- [AI usage](#ai-usage)
- [Git workflow](#git-workflow)
- [Definition of done](#definition-of-done)

---

## What the project does

A sender reads a file, splits it into MSS-sized datagrams, and hands each one
to a transport. The transport encapsulates it in a DATA packet and keeps
retransmitting until the receiver acknowledges it. The receiver verifies each
packet, delivers it once and in order, and reassembles the file. Finally,
both sides compare **SHA-256** hashes.

The channel between them is deliberately hostile. It can:

- **lose** packets,
- **delay** them,
- add **jitter**,
- **duplicate** them,
- **reorder** them,
- **corrupt** their bytes.

The ARQ protocols are what make delivery reliable despite all of that.

The project uses only the Python standard library at runtime. **pytest** is
needed to run the test suite.

---

## Quick start

From the repository root on Windows:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe demo.py
```

Or with a normal Python installation:

```bash
python -m pytest -q
python demo.py
```

Expected result:

```text
123 passed
```

The full suite takes roughly two minutes because it performs many real UDP
transfers with real timers. For a quick demonstration, use `demo.py`.

### Run one suite

```powershell
.\.venv\Scripts\python.exe -m pytest tests\test_packet.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_stop_and_wait.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_go_back_n.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_selective_repeat.py -q
.\.venv\Scripts\python.exe -m pytest tests\test_correctness.py -q
```

### Run one test

```powershell
.\.venv\Scripts\python.exe -m pytest "tests\test_correctness.py::test_file_transfer_sha256_matches" -q
```

In VS Code, `Terminal → Run Task → full-suite-verify` runs the whole suite.

---

## Reliability demo

`demo.py` sends a 16 KiB file (16 chunks) through a channel with **25% loss**
and compares the received file with the original.

Example output:

```text
Channel loss    : 25%

Protocol           Chunks Dropped DataPkts Received Resends SHA-256   Time
--------------------------------------------------------------------------
Null (no ARQ)          16       5       16       11       0    FAIL  0.31s
Stop-and-Wait          16       9       25       16       9    PASS  0.98s
Go-Back-N              16      19       53       16      37    PASS  1.14s
Selective Repeat       16      11       31       16      15    PASS  0.60s
```

How to read it:

| Column | Meaning |
| --- | --- |
| **Chunks** | Number of original pieces in the file |
| **Dropped** | DATA packets discarded by the loss model |
| **DataPkts** | Total DATA packets sent, including retransmissions |
| **Received** | Chunks delivered to the receiver |
| **Resends** | `DataPkts - Chunks` — extra packets caused by recovery |
| **SHA-256** | Whether the reassembled file matches the original |
| **Time** | Wall-clock transfer time |

What it proves:

1. **NullTransport** has no acknowledgement or retransmission, so dropped
   packets stay lost: only 11 of 16 chunks arrive and the hash fails.
2. **Stop-and-Wait, Go-Back-N, and Selective Repeat** recover every lost
   chunk. All 16 arrive and the SHA-256 matches.
3. The extra DATA packets are retransmissions triggered by loss.

> `Resends` is not a fair cross-protocol score by itself. Each protocol
> sees a different loss pattern because it sends in a different order, and a
> single run is noisy. See [Protocol comparison](#protocol-comparison) for
> the trade-offs.

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────┐
│ Application / tests                                          │
│   chunk file → send chunks → receive chunks → verify SHA-256  │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│ Transport (udp_arq.transport)                                │
│   NullTransport | Stop-and-Wait | Go-Back-N | Selective Repeat│
│   reliability, sequencing, ACKs, retransmission, timers       │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│ Channel emulator (udp_arq.channel)                            │
│   loss · delay · jitter · duplication · reorder · corruption  │
└──────────────────────────────┬───────────────────────────────┘
                               │
┌──────────────────────────────▼───────────────────────────────┐
│ Real UDP socket (socket.SOCK_DGRAM)                           │
└──────────────────────────────────────────────────────────────┘
```

The channel is a **shim, not a relay**: it wraps the sender's real UDP socket
and applies impairments on the send path only. The receive path reads directly
from the socket.

| Module | Role |
| --- | --- |
| `udp_arq/packet.py` | 16-byte header encode/decode and validation |
| `udp_arq/checksum.py` | RFC 1071 one's-complement checksum |
| `udp_arq/file_layer.py` | Chunking, reassembly, SHA-256 |
| `udp_arq/channel.py` | UDP emulator with impairments and counters |
| `udp_arq/rto.py` | Jacobson/Karels adaptive RTO estimator |
| `udp_arq/timer_heap.py` | Per-packet timeout heap |
| `udp_arq/transport/base.py` | Frozen Transport ABC |
| `udp_arq/transport/null.py` | Best-effort passthrough (no ARQ) |
| `udp_arq/transport/stop_and_wait.py` | Alternating-bit ARQ |
| `udp_arq/transport/go_back_n.py` | Sliding window, cumulative ACK |
| `udp_arq/transport/selective_repeat.py` | Sliding window, individual ACK |

---

## Wire format (FROZEN)

Every packet has a fixed **16-byte header**, big-endian (`!`), followed by the
payload verbatim. `struct` format: **`!HIIBBHH`**.

| Offset | Size | Field | Type | Meaning |
| ---: | ---: | --- | --- | --- |
| 0 | 2 | `checksum` | uint16 | One's-complement checksum; field is zero while computing |
| 2 | 4 | `seq` | uint32 | Sequence number of this packet |
| 6 | 4 | `ack` | uint32 | ACK number (see below) |
| 10 | 1 | `flags` | uint8 | `0x01` DATA, `0x02` ACK, `0x04` FIN |
| 11 | 1 | `version` | uint8 | Must equal `PROTOCOL_VERSION` (1) |
| 12 | 2 | `payload_len` | uint16 | Bytes after the header; must equal `len(payload)` |
| 14 | 2 | `magic` | uint16 | Must equal `MAGIC` (`0xA55A`) |

### ACK interpretation

The header layout is frozen; the ACK field is interpreted by each protocol:

- **Stop-and-Wait:** `ack` is the next expected sequence number, so an ACK for
  sequence `seq` is `seq ^ 1`.
- **Go-Back-N:** `ack` is cumulative — the next sequence number expected.
- **Selective Repeat:** `ack = seq + 1` identifies the individual packet that
  was received.

### Limits

- `MAX_PAYLOAD = 65491` bytes: the maximum UDP/IPv4 payload is 65507 bytes,
  minus the 16-byte header.
- `DEFAULT_MSS = 1024` bytes: the default application chunk size. This is a
  chunking choice, not the network MTU.
- Sequence and ACK numbers are 32-bit.

### Validation

`decode()` raises `PacketError` when:

- the buffer is shorter than 16 bytes,
- the magic or version is wrong,
- unknown flag bits are set,
- `payload_len` disagrees with the remaining bytes.

Non-bytes input raises `TypeError`. `encode()` also rejects a header whose
`payload_len` does not match the payload.

---

## Checksum

`udp_arq/checksum.py` implements the RFC 1071 16-bit one's-complement
checksum:

- the checksum field is zeroed before calculation,
- 16-bit words are added big-endian,
- carries are folded,
- odd-length input is padded with one zero byte,
- the result is complemented.

API:

```python
checksum(buf: bytes) -> int
verify(buf: bytes) -> bool
```

`verify()` reads the stored value, zeros bytes 0–1, recomputes, and compares.
Tests confirm that every single-bit flip in a 1000-packet sample is caught.

---

## Channel emulator

`udp_arq/channel.py` wraps one real UDP socket.

```python
class Channel:
    def __init__(
        self,
        local_addr: tuple[str, int],
        *,
        loss: float = 0.0,
        delay: float = 0.0,
        jitter: float = 0.0,
        duplication: float = 0.0,
        reorder: float = 0.0,
        corruption: float = 0.0,
        seed: int | None = None,
    ): ...

    def sendto(self, data: bytes, addr: tuple[str, int]) -> None: ...
    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, tuple[str, int]]: ...
    def close(self) -> None: ...

    @property
    def local_addr(self) -> tuple[str, int]: ...
    @property
    def sent(self) -> int: ...
    @property
    def dropped(self) -> int: ...
    @property
    def delivered(self) -> int: ...
```

Behaviour:

- Impairments are applied on the **send path only**.
- Delivery is scheduled on a min-heap and performed by a background thread.
- `recvfrom()` raises `TimeoutError` on expiry; `timeout=None` blocks.
- One seeded `random.Random` drives everything.
- Every `sendto()` consumes exactly **five draws** in this fixed order:
  **loss, jitter, duplication, reorder, corruption**.
- All five draws are consumed even when the packet is dropped, so the same
  seed gives the same impairment sequence on every run.
- `sent` counts accepted sends; `dropped` counts lost packets; `delivered`
  counts real socket sends. With duplication, `delivered` can exceed `sent`.
- On Windows, `SIO_UDP_CONNRESET` is disabled so an ICMP “port unreachable”
  does not break the next `recvfrom()`.

Determinism note: the same seed gives the same **impairment decisions**, but
not necessarily the same **arrival order**, because delivery deadlines are
absolute wall-clock times and the delivery thread is scheduled by the OS.
Tests therefore compare drop flags, delivered counts, and the sorted multiset
of payloads.

---

## Transport interface

`udp_arq/transport/base.py` defines the frozen datagram API:

```python
Address = tuple[str, int]

class Transport(ABC):
    def __init__(self, channel, *, mss: int = DEFAULT_MSS,
                 timeout: float = 0.5): ...

    @abstractmethod
    def sendto(self, data: bytes, addr: Address) -> None: ...

    @abstractmethod
    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]: ...

    @abstractmethod
    def close(self) -> None: ...
```

A transport reliably delivers **one datagram of at most `mss` bytes** per
`sendto`. Chunking larger data is the file layer's job.

### Sending semantics

- **Stop-and-Wait** blocks inside `sendto()` until that datagram is ACKed.
- **Go-Back-N and Selective Repeat** queue the datagram and return, so the
  window can fill. Callers that need confirmation call `flush()`:
  `flush(addr=None, timeout=60.0)` waits until every queued datagram is
  acknowledged, raises the first delivery error, and raises `TimeoutError`
  if the deadline passes.
- All transports raise `ValueError` if `len(data) > mss`.
- GBN and SR raise `RuntimeError` after `close()`; Stop-and-Wait and
  NullTransport surface the closed channel's error.
- Stop-and-Wait, GBN, and SR raise `TimeoutError` after the retry cap
  (`MAX_RETRIES = 1000`). NullTransport has no retry loop.

---

## Protocols

### NullTransport

Zero-retransmit passthrough. It proves the interfaces end to end and is the
**negative control** for reliability: whatever the channel loses stays lost.

### Stop-and-Wait

Classic alternating-bit protocol:

- one DATA packet in flight per peer,
- sequence numbers alternate `0/1` per destination,
- sender retransmits on timeout until `ack == seq ^ 1`,
- receiver delivers a fresh in-order packet and ACKs the next expected bit,
- a duplicate is re-ACKed but **not** delivered twice,
- corrupted or malformed packets are silently dropped.

### Go-Back-N

Sliding window with cumulative ACKs:

- `window_size` defaults to 4,
- `sendto()` queues and returns; `flush()` waits for cumulative ACKs,
- one timer for the oldest outstanding packet,
- on timeout, retransmit **every** outstanding packet in the window,
- receiver accepts in-order DATA only, re-ACKs duplicates, and does not
  buffer out-of-order packets.

### Selective Repeat

Sliding window with individual ACKs:

- `window_size` defaults to 4,
- `sendto()` queues and returns; `flush()` waits for delivery,
- receiver buffers out-of-order DATA and delivers it in sequence order,
- per-packet timers are managed by `TimerHeap`,
- on timeout, retransmit **only** the missing packets,
- duplicates below the expected sequence are re-ACKed but not redelivered.

### Protocol comparison

| Property | Stop-and-Wait | Go-Back-N | Selective Repeat |
| --- | --- | --- | --- |
| Packets in flight | 1 | up to `window_size` | up to `window_size` |
| ACK style | alternating bit / next expected | cumulative | individual (`seq + 1`) |
| Timeout retransmits | current packet | whole outstanding window | only missing packets |
| Out-of-order handling | drop / re-ACK | drop / re-ACK | buffer and deliver in order |
| Timers | one per attempt | one for the window base | one per packet |
| Relative strength | simplest, packet-efficient | simple, but wasteful on loss | best overall throughput/efficiency |

A representative benchmark (24 KiB, 10% loss, 10 ms delay, average of three
seeds) shows the trade-off:

| Protocol | Avg time | Avg DATA packets | Avg resends |
| --- | ---: | ---: | ---: |
| Stop-and-Wait | 0.63 s | 28.0 | 4.0 |
| Go-Back-N | 0.38 s | 37.3 | 13.3 |
| Selective Repeat | **0.29 s** | 31.0 | 7.0 |

Stop-and-Wait sends the fewest packets but cannot pipeline. Go-Back-N is
faster but resends whole windows. Selective Repeat is the fastest and sends
fewer packets than Go-Back-N.

---

## Adaptive RTO

`udp_arq/rto.py` implements a simplified RFC 6298 estimator:

```text
first sample : SRTT = RTT, RTTVAR = RTT / 2
later        : RTTVAR = (1 - beta) * RTTVAR + beta * |SRTT - RTT|
               SRTT   = (1 - alpha) * SRTT + alpha * RTT
RTO          = SRTT + max(G, K * RTTVAR)   clamped to [min_rto, max_rto]
on timeout   : RTO = min(max_rto, 2 * RTO)
```

Constants: `alpha = 0.125`, `beta = 0.25`, `K = 4`,
`G = 0.05 s`, `min_rto = 0.1 s`, `max_rto = 5.0 s`.

Pass `adaptive=True` to any transport:

```python
StopAndWaitTransport(channel, timeout=0.5, adaptive=True)
GoBackNTransport(channel, timeout=0.5, window_size=4, adaptive=True)
SelectiveRepeatTransport(channel, timeout=0.5, window_size=4, adaptive=True)
```

**Karn's algorithm** is applied in all three transports: an ACK for a
retransmitted packet is ambiguous, so it never produces an RTT sample. The
timer backs off on timeout and resumes sampling only after a clean ACK.

The default fixed-timeout path is unchanged when `adaptive=False`.

---

## File layer

`udp_arq/file_layer.py`:

| Function | Purpose |
| --- | --- |
| `chunk(data, mss=1024)` | Split bytes into MSS-sized pieces |
| `reassemble(chunks)` | Join ordered chunks back into bytes |
| `sha256_hex(data)` | SHA-256 hex digest of bytes |
| `read_file_chunks(path, mss)` | Stream a file in chunks without loading it all |
| `reassemble_file(chunks, path)` | Write chunks to a destination file |
| `sha256_file(path, chunk_size=1 MiB)` | Hash a file incrementally |
| `files_match(a, b)` | Compare two files by SHA-256 |

The transfer flow is:

1. chunk the file,
2. `sendto()` each chunk through the transport,
3. `recvfrom()` until every chunk arrives,
4. reassemble,
5. compare SHA-256 on both ends.

---

## Repository layout

```text
udp-arq-lab/
├── README.md                 # this file
├── AI-USE.md                 # AI usage policy and log
├── demo.py                   # fast 25%-loss reliability demo
├── conftest.py               # adds repo root to sys.path for pytest
├── udp_arq/
│   ├── __init__.py
│   ├── packet.py             # 16-byte wire format
│   ├── checksum.py           # RFC 1071 checksum
│   ├── file_layer.py         # chunking, reassembly, SHA-256
│   ├── channel.py            # UDP channel emulator
│   ├── rto.py                # adaptive RTO estimator
│   ├── timer_heap.py         # per-packet timers
│   └── transport/
│       ├── __init__.py
│       ├── base.py           # frozen Transport ABC
│       ├── null.py           # no-ARQ passthrough
│       ├── stop_and_wait.py  # alternating-bit ARQ
│       ├── go_back_n.py      # sliding window, cumulative ACK
│       └── selective_repeat.py  # sliding window, individual ACK
└── tests/
    ├── test_packet.py
    ├── test_checksum.py
    ├── test_file_layer.py
    ├── test_channel.py
    ├── test_transport.py
    ├── test_stop_and_wait.py
    ├── test_go_back_n.py
    ├── test_selective_repeat.py
    ├── test_timer_heap.py
    ├── test_rto.py
    └── test_correctness.py
```

---

## Testing

Run everything:

```bash
python -m pytest -q
```

Current result: **123 passed**.

| Test file | Covers |
| --- | --- |
| `test_packet.py` | Header round trips, empty/max payloads, malformed input |
| `test_checksum.py` | One's-complement checksum and bit-flip detection |
| `test_file_layer.py` | Chunk/reassemble, SHA-256, file helpers |
| `test_channel.py` | Loss, delay, jitter, duplication, reorder, corruption, determinism |
| `test_transport.py` | Frozen interface and NullTransport harness |
| `test_stop_and_wait.py` | Round trip, loss recovery, duplicates, file SHA-256 |
| `test_go_back_n.py` | Window behaviour, pipelining, loss recovery, failure path |
| `test_selective_repeat.py` | Out-of-order buffering, selective retransmit, failure path |
| `test_timer_heap.py` | Timer scheduling, cancellation, expiry order |
| `test_rto.py` | SRTT/RTTVAR math, backoff, clamps, Karn behaviour |
| `test_correctness.py` | Every protocol at 0%/10% loss, adaptive RTO, duplicates, oversized payloads |

The tests use real UDP sockets and real threads. The channel is seeded, and
timing-sensitive assertions use order-insensitive comparisons or generous
bounds so results are repeatable.

---

## Design decisions

**Why UDP instead of TCP?**
The point of the lab is to build reliability ourselves. UDP gives a clean
unreliable datagram service on which ARQ behaviour is visible.

**Why freeze the header?**
Three transports are written against one wire format. Changing it after the
kickoff would force simultaneous edits across all transports.

**Why is the channel a shim, not a relay?**
A shim needs one socket per endpoint and no forwarding process. It applies
impairments exactly where the protocol sends, while still using a real UDP
socket.

**Why a magic number and version?**
They reject foreign or malformed datagrams quickly and make protocol
mismatches explicit.

**Why 65491 bytes?**
`65507` is the maximum UDP/IPv4 payload; subtracting the 16-byte header gives
`65491`.

**Why is the default MSS 1024?**
It keeps datagrams small, avoids IP fragmentation, and limits the cost of a
lost or corrupted retransmission. It is an application chunk size, not the
network MTU.

**Why checksum and SHA-256?**
The checksum catches per-packet corruption cheaply. SHA-256 verifies the
whole reassembled file end to end.

**Why does GBN/SR `sendto()` return immediately?**
Blocking on each datagram would cap the pipeline at one packet and reduce GBN
and SR to Stop-and-Wait. `flush()` provides the blocking delivery guarantee
for a batch.

**How are duplicate DATA packets handled?**
Sequence numbers detect them. Stop-and-Wait and GBN re-ACK without
redelivering; SR re-ACKs duplicates below the expected sequence.

**How are multiple peers handled?**
Sequence state, windows, timers, ACK state, and RTO estimators are keyed by
peer address.

**How are permanent failures handled?**
After `MAX_RETRIES = 1000`, Stop-and-Wait, GBN, and SR raise
`TimeoutError`. GBN and SR also keep a sticky per-peer error so a later
`flush()` still reports the failure instead of silently succeeding.

---

## Limitations and next steps

- `FLAG_FIN` is defined but connection teardown is not implemented.
- Selective Repeat uses individual ACKs, not a SACK bitmap.
- No congestion control; the window is fixed.
- `RTOEstimator` uses a default `max_rto` of 5 seconds. A caller using
  `adaptive=True` with `timeout > 5` should also raise `max_rto`.
- The socket layer is IPv4 (`AF_INET`).
- No throughput/loss plots are generated; the benchmark tables above are
  produced by small scripts.

---

## Team and ownership

| Who | Main contributions |
| --- | --- |
| **Satwik** | Wire format and packet tests; Stop-and-Wait; channel restoration and payload-limit fix; GBN deadlock and pipelining fix; adaptive RTO wiring for Stop-and-Wait and GBN; final GBN/SR failure handling, cross-protocol coverage, docs, and integration |
| **Pratik** | Checksum; file layer; initial Go-Back-N; adaptive RTO estimator; cross-protocol correctness suite |
| **Ahana** | Channel emulator v1 and v2: loss, delay, jitter, duplication, reordering, corruption, determinism |
| **Sagnik** | Transport interface; NullTransport; TimerHeap; Selective Repeat transport and tests |

Everyone reviewed and ran the suite before merging.

---

## AI usage

This repository follows the policy and log in [AI-USE.md](AI-USE.md).

In short: AI may assist with drafting, debugging, and review, but every
committed line must be understood by a human, AI-assisted work must be
reviewed, and significant use must be logged. Test results must be real.

---

## Git workflow

- One branch per feature or owner.
- Pull requests are reviewed and merged into `main`.
- Run the full suite after every merge.
- Commit with your own name and email; never commit with a teammate's
  identity.

---

## Definition of done

| Milestone | Status |
| --- | --- |
| 16-byte wire format, validation, packet tests | ✅ |
| RFC 1071 checksum with bit-flip coverage | ✅ |
| File chunking, reassembly, SHA-256 | ✅ |
| Channel emulator with v1 and v2 impairments | ✅ |
| Frozen Transport interface and NullTransport | ✅ |
| Stop-and-Wait at 0% and 10% loss | ✅ |
| Go-Back-N with pipelining and loss recovery | ✅ |
| Selective Repeat with out-of-order buffering | ✅ |
| Adaptive RTO in all three transports | ✅ |
| Cross-protocol correctness suite | ✅ |
| Failure-path handling (`TimeoutError`, sticky errors) | ✅ |
| Full suite | ✅ 123 passed |