import random

from udp_arq.checksum import checksum, verify


def make_packet(payload: bytes) -> bytes:
    """
    Create a packet with the checksum stored
    in bytes 0-1.
    """

    # Reserve the first 2 bytes for checksum.
    packet = bytearray(b"\x00\x00" + payload)

    # Calculate checksum with bytes 0-1 zeroed.
    value = checksum(packet)

    # Store checksum in bytes 0-1.
    packet[0:2] = value.to_bytes(2, byteorder="big")

    return bytes(packet)


def test_valid_packet():
    packet = make_packet(b"hello UDP ARQ")

    assert verify(packet)


def test_corrupted_packet():
    packet = make_packet(b"hello UDP ARQ")

    corrupted = bytearray(packet)

    # Flip one bit in the payload.
    corrupted[2] ^= 0x01

    assert not verify(bytes(corrupted))


def test_empty_payload():
    packet = make_packet(b"")

    assert verify(packet)


def test_odd_length_payload():
    packet = make_packet(b"abcde")

    assert verify(packet)


def test_1000_random_packets_single_bit_flips():
    rng = random.Random(12345)

    for _ in range(1000):

        # Generate a random payload.
        payload_length = rng.randint(1, 512)

        payload = bytes(
            rng.getrandbits(8)
            for _ in range(payload_length)
        )

        # Create valid packet.
        packet = make_packet(payload)

        # Original packet must pass.
        assert verify(packet)

        # Flip every individual bit in the packet.
        for byte_index in range(len(packet)):

            for bit_index in range(8):

                corrupted = bytearray(packet)

                corrupted[byte_index] ^= (1 << bit_index)

                # Every single-bit corruption must be detected.
                assert not verify(bytes(corrupted))