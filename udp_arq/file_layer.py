"""
File chunking, reassembly, and SHA-256 utilities for UDP ARQ.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, Iterator


DEFAULT_HASH_CHUNK_SIZE = 1024 * 1024


def chunk(data: bytes, mss: int = 1024) -> list[bytes]:
    """
    Split data into MSS-sized chunks.

    Args:
        data: Data to split.
        mss: Maximum size of each chunk in bytes.

    Returns:
        A list containing the chunks.

    Raises:
        TypeError: If data is not bytes-like.
        ValueError: If mss is not positive.
    """

    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")

    if mss <= 0:
        raise ValueError("mss must be greater than zero")

    data = bytes(data)

    return [
        data[start:start + mss]
        for start in range(0, len(data), mss)
    ]


def reassemble(chunks: Iterable[bytes]) -> bytes:
    """
    Reassemble chunks into the original data.

    Args:
        chunks: Chunks in the correct order.

    Returns:
        Reassembled bytes.
    """

    return b"".join(bytes(part) for part in chunks)


def sha256_hex(data: bytes) -> str:
    """
    Calculate SHA-256 and return it as a hexadecimal string.

    Args:
        data: Data to hash.

    Returns:
        SHA-256 hexadecimal digest.
    """

    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")

    return hashlib.sha256(bytes(data)).hexdigest()


def read_file_chunks(
    path: str | Path,
    mss: int,
) -> Iterator[bytes]:
    """
    Read a file incrementally in MSS-sized chunks.

    This avoids loading the entire file into memory.

    Args:
        path: Path to the input file.
        mss: Maximum size of each chunk.

    Yields:
        File chunks of at most MSS bytes.
    """

    if mss <= 0:
        raise ValueError("mss must be greater than zero")

    path = Path(path)

    with path.open("rb") as file:
        while True:
            data = file.read(mss)

            if not data:
                break

            yield data


def reassemble_file(
    chunks: Iterable[bytes],
    output_path: str | Path,
) -> None:
    """
    Write chunks to an output file.

    Args:
        chunks: Chunks in the correct order.
        output_path: Destination file path.
    """

    output_path = Path(output_path)

    with output_path.open("wb") as file:
        for data in chunks:
            file.write(bytes(data))


def sha256_file(
    path: str | Path,
    chunk_size: int = DEFAULT_HASH_CHUNK_SIZE,
) -> str:
    """
    Calculate SHA-256 of a file without loading
    the entire file into memory.

    Args:
        path: Path to the file.
        chunk_size: Number of bytes read at a time.

    Returns:
        SHA-256 hexadecimal digest.
    """

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    path = Path(path)

    digest = hashlib.sha256()

    with path.open("rb") as file:
        while True:
            data = file.read(chunk_size)

            if not data:
                break

            digest.update(data)

    return digest.hexdigest()


def files_match(
    source_path: str | Path,
    destination_path: str | Path,
) -> bool:
    """
    Compare two files using SHA-256.

    Returns:
        True if both files have the same SHA-256 hash.
    """

    return sha256_file(source_path) == sha256_file(destination_path)