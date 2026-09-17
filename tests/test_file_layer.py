import os

from udp_arq.file_layer import (
    chunk,
    reassemble,
    sha256_hex,
)


def test_10_mib_chunk_reassemble_hash():
    # Generate 10 MiB of random data.
    data = os.urandom(10 * 1024 * 1024)

    # Split into MSS-sized chunks.
    chunks = chunk(data, mss=1024)

    # Every chunk must be <= MSS.
    assert all(len(part) <= 1024 for part in chunks)

    # Reassemble the chunks.
    reconstructed = reassemble(chunks)

    # Original and reconstructed data must be identical.
    assert reconstructed == data

    # SHA-256 hashes must match.
    assert sha256_hex(reconstructed) == sha256_hex(data)