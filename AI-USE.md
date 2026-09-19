# AI Use Policy

This repository may be developed with AI assistance (GitHub Copilot, LLMs)
under the rules below. Course policy takes precedence over this file.

## Allowed

- Drafting documentation, interface stubs, and test skeletons.
- Debugging, explaining concepts, reviewing code for bugs.

## Required

- Every team member must be able to explain every line they commit.
- AI-suggested code must be reviewed and understood before it is committed.
- Significant AI-generated content is logged below.

## Forbidden

- Committing code nobody reviewed or understands.
- Fabricated test results or fake evidence of correctness.
- Anything the course syllabus forbids.

## Log

| Date | Who | Files | Tool | Summary |
| --- | --- | --- | --- | --- |
| 2026-09-15 | Satwik | `udp_arq/packet.py`, `tests/test_packet.py`, `README.md` | GitHub Copilot | Wire format, self-tests, frozen-spec draft |
| 2026-09-17 | Pratik | `udp_arq/checksum.py`, `udp_arq/file_layer.py`, tests | ChatGPT | Renamed to match spec, added checksum/file-layer implementation and tests |
| 2026-09-19 | Satwik | `udp_arq/channel.py`, `tests/test_channel.py`, `udp_arq/packet.py`, `README.md`, `.gitignore` | Claude Code | Fixed branch/ahana's PR shipping an empty `channel2.py` by restoring the tested implementation; corrected `MAX_PAYLOAD` (65535 → 65491, was larger than one UDP/IPv4 datagram); untracked `.vscode/settings.json` |
| 2026-09-19 | Satwik | `udp_arq/transport/stop_and_wait.py`, `tests/test_stop_and_wait.py`, `README.md` | Claude Code | Stop-and-Wait ARQ transport (alternating-bit protocol) and tests; week 3 target — file transfer through the emulator at 0% and 10% loss with SHA-256 match |
