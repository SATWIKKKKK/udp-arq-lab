"""
File chunking, reassembly, and SHA-256 utilities for UDP ARQ.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Iterable, Iterator


DEFAULT_HASH_CHUNK_SIZE = 1024 * 1024  # 1 MiB


def chunk_bytes(data: bytes, mss: int) -> Iterator[bytes]:
    """
    Split bytes into chunks of at most MSS bytes.

    Args:
        data: Data to split.
        mss: Maximum payload size in bytes.

    Yields:
        MSS-sized payload chunks.

    Raises:
        TypeError: If data is not bytes-like.
        ValueError: If MSS is not positive.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")

    if mss <= 0:
        raise ValueError("MSS must be greater than zero")

    data = bytes(data)

    for start in range(0, len(data), mss):
        yield data[start:start + mss]


def read_file_chunks(
    path: str | Path,
    mss: int,
) -> Iterator[bytes]:
    """
    Read a file incrementally and yield MSS-sized payloads.

    The whole file is not loaded into memory.

    Args:
        path: Input file path.
        mss: Maximum payload size in bytes.

    Yields:
        File chunks of at most MSS bytes.

    Raises:
        ValueError: If MSS is not positive.
        FileNotFoundError: If the file does not exist.
    """
    if mss <= 0:
        raise ValueError("MSS must be greater than zero")

    path = Path(path)

    with path.open("rb") as file:
        while True:
            chunk = file.read(mss)

            if not chunk:
                break

            yield chunk


def reassemble_bytes(chunks: Iterable[bytes]) -> bytes:
    """
    Reassemble payload chunks into the original byte sequence.

    Args:
        chunks: Payload chunks in their correct order.

    Returns:
        Reassembled bytes.
    """
    return b"".join(bytes(chunk) for chunk in chunks)


def reassemble_file(
    chunks: Iterable[bytes],
    output_path: str | Path,
) -> None:
    """
    Write payload chunks directly to an output file.

    Args:
        chunks: Payload chunks in their correct order.
        output_path: Destination file path.
    """
    output_path = Path(output_path)

    with output_path.open("wb") as file:
        for chunk in chunks:
            file.write(bytes(chunk))


def sha256_bytes(data: bytes) -> str:
    """
    Return the SHA-256 hash of bytes as a hexadecimal string.
    """
    if not isinstance(data, (bytes, bytearray, memoryview)):
        raise TypeError("data must be bytes-like")

    return hashlib.sha256(bytes(data)).hexdigest()


def sha256_file(
    path: str | Path,
    chunk_size: int = DEFAULT_HASH_CHUNK_SIZE,
) -> str:
    """
    Calculate the SHA-256 hash of a file without loading it
    completely into memory.

    Args:
        path: File to hash.
        chunk_size: Number of bytes read per iteration.

    Returns:
        SHA-256 hexadecimal digest.

    Raises:
        ValueError: If chunk_size is not positive.
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    path = Path(path)

    digest = hashlib.sha256()

    with path.open("rb") as file:
        while True:
            chunk = file.read(chunk_size)

            if not chunk:
                break

            digest.update(chunk)

    return digest.hexdigest()


def files_match(
    source_path: str | Path,
    destination_path: str | Path,
) -> bool:
    """
    Compare two files using SHA-256.
    """
    return sha256_file(source_path) == sha256_file(destination_path)