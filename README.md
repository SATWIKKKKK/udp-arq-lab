# udp-arq-lab

Reliable data transfer over an unreliable UDP channel. **Week 3 target:**
Stop-and-Wait through the emulator at 0% and then 10% loss, with SHA-256
matching on both ends. Go-Back-N and Selective Repeat follow in later weeks.

## Team and ownership

| Who | Owns | Files |
| --- | --- | --- |
| Satwik | wire format | `udp_arq/packet.py` |
| Pratik | checksum + file layer | `udp_arq/checksum.py`, `udp_arq/file_layer.py` |
| Ahana | channel emulator v1 | `udp_arq/channel.py` |
| Sagnik | transport interface + tests | `udp_arq/transport/base.py`, `udp_arq/transport/null.py`, `tests/` |

## Frozen decisions (kickoff, Sept 15 2026)

1. **The emulator is a shim, not a relay.** `Channel` wraps a real UDP
   socket; impairment (loss/delay/jitter) is applied on the send path.
2. **The header layout below is frozen.** A header change after this week
   means three transports edited by three people at once.
3. **The Transport and Channel interfaces below are frozen.** Nobody writes
   a protocol implementation against an unfrozen spec.

Rule: code against this README, never against a teammate's file.

## Wire format (FROZEN)

Fixed 16-byte header, all fields big-endian (`struct` format string
`!HIIBBHH`), payload appended verbatim after the header.

| Offset | Size | Field | Type | Meaning |
| --- | --- | --- | --- | --- |
| 0 | 2 | checksum | uint16 | one's-complement over the whole packet; field zeroed while computing |
| 2 | 4 | seq | uint32 | sequence number of this packet |
| 6 | 4 | ack | uint32 | next expected sequence number (cumulative ACK) |
| 10 | 1 | flags | uint8 | `0x01` DATA, `0x02` ACK, `0x04` FIN |
| 11 | 1 | version | uint8 | must equal `PROTOCOL_VERSION` (1) |
| 12 | 2 | payload_len | uint16 | bytes after the header; must equal `len(payload)` |
| 14 | 2 | magic | uint16 | must equal `MAGIC` (`0xA55A`) |

Limits: payload 0 .. `MAX_PAYLOAD` = 65491 bytes (65507-byte UDP/IPv4
datagram ceiling minus the 16-byte header); default `MSS` = 1024.

`decode()` raises `PacketError` for: buffer shorter than 16 bytes, wrong
magic, wrong version, unknown flag bits, or `payload_len` that disagrees
with the remaining length. Non-bytes input raises `TypeError`.

## Checksum (frozen contract — Pratik)

RFC 1071 16-bit one's complement: checksum field zeroed first, carries
folded, odd-length input padded with one zero byte.

- `checksum(buf) -> int`
- `verify(buf) -> bool` — recompute with the field zeroed and compare.

Done when: every single-bit flip across a 1000-packet sample is caught.

## Channel emulator (frozen API — Ahana)

`Address = tuple[str, int]` (host, port — defined in `transport/base.py`)

```python
class Channel:
    def __init__(self, local_addr: Address, *,
                 loss: float = 0.0, delay: float = 0.0,
                 jitter: float = 0.0, seed: int | None = None): ...
    def sendto(self, data: bytes, addr: Address) -> None: ...
    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]: ...
    def close(self) -> None: ...
    # read-only counters: sent, dropped, delivered
```

- `recvfrom` raises `TimeoutError` on expiry; `timeout=None` blocks forever.
- One seeded `random.Random`; heap of `(deliver_at, packet)` entries on the
  send side.
- Done when: the same seed gives a byte-identical drop sequence in two runs,
  and measured loss over 10k packets is within ~1% of configured.

## Transport interface (frozen — Sagnik)

Datagram API: a transport reliably delivers **one datagram of at most `mss`
bytes** per `sendto` call. Chunking above MSS is the file layer's job, not
the transport's.

```python
class Transport(ABC):
    def __init__(self, channel: Channel, *,
                 mss: int = DEFAULT_MSS, timeout: float = 0.5): ...

    @abstractmethod
    def sendto(self, data: bytes, addr: Address) -> None: ...
    # Reliably deliver one datagram (len(data) <= mss). Blocks and
    # retransmits until acknowledged. Raises ValueError if len > mss.

    @abstractmethod
    def recvfrom(self, timeout: float | None = None) -> tuple[bytes, Address]: ...
    # Next received datagram + sender. Raises TimeoutError on expiry.

    @abstractmethod
    def close(self) -> None: ...
```

`NullTransport` is the zero-retransmit passthrough that proves this
interface end-to-end before any ARQ logic exists.

## Stop-and-Wait transport (Satwik)

`StopAndWaitTransport(Transport)`: classic alternating-bit protocol, one
packet in flight per peer address at a time.

- `sendto`: sends the DATA packet, then blocks until an ACK with
  `ack == seq ^ 1` arrives or the per-attempt `timeout` expires, in which
  case it retransmits the same packet. Sequence numbers alternate 0/1 per
  destination address.
- `recvfrom`: on a fresh in-order DATA packet, delivers the payload and
  ACKs with the next expected sequence number. On a duplicate (the
  previous ACK was lost, not the data), it re-sends that same ACK but does
  not redeliver the payload.
- Corrupted or malformed packets (bad checksum, `PacketError`) are
  silently dropped on both sides, same as `NullTransport`.

Done when: Stop-and-Wait moves a file through the emulator at 0% loss,
then 10% loss, with SHA-256 matching on both ends
(`tests/test_stop_and_wait.py::test_file_transfer_sha256_matches`).

## File layer + transfer harness (Pratik + Sagnik)

- `chunk(data, mss=DEFAULT_MSS) -> list[bytes]`
- `reassemble(chunks) -> bytes`
- `sha256_hex(data) -> str`

Harness flow: sender chunks the file and `sendto`s each chunk in order;
receiver `recvfrom`s until it has every chunk, reassembles, and asserts
SHA-256 equality on both ends.

## Layout

```text
udp-arq-lab/
├── README.md             # frozen spec (this file)
├── AI-USE.md             # AI usage policy + log
├── conftest.py           # puts repo root on sys.path for pytest
├── udp_arq/
│   ├── __init__.py
│   ├── packet.py         # Satwik — wire format
│   ├── checksum.py       # Pratik
│   ├── file_layer.py     # Pratik
│   ├── channel.py        # Ahana
│   └── transport/
│       ├── __init__.py
│       ├── base.py             # Sagnik — frozen interface
│       ├── null.py             # Sagnik — passthrough proof
│       └── stop_and_wait.py    # Satwik — week 3 target
└── tests/                # Pratik & Sagnik (+ packet/channel/S&W self-tests)
```

## Run tests

```bash
python -m pytest -q
```

## Definition of done

- **Satwik** — ✅ round-trip holds for empty, random, and max payloads;
  garbage and truncated input raises cleanly. (Self-tests: `tests/test_packet.py`)
- **Pratik** — ✅ all single-bit flips in a 1000-packet sample caught; 10 MiB
  file survives chunk → reassemble with matching SHA-256.
- **Ahana** — ✅ same seed ⇒ identical drop sequence twice; 10k packets
  within ~1% of configured loss. (`tests/test_channel.py`)
- **Sagnik** — ✅ `pytest` green; null transport moves a file through the
  emulator at 0% loss. (`tests/test_transport.py::test_file_transfer_harness`)
- **Integration checkpoint (day 5)** — ✅ everything merges; null transport
  carries a real file at 0% loss with hashes matching. That is week 3's
  start line.
- **Week 3 target** — ✅ Stop-and-Wait at 0%, then 10% loss, SHA-256
  matching. (`tests/test_stop_and_wait.py::test_file_transfer_sha256_matches`)

## Git rules (from kickoff)

- Four kickoff commits, one per person, each under their own name and email.
  Check `git config user.email` on every machine first.
- Never commit with a teammate's identity.
