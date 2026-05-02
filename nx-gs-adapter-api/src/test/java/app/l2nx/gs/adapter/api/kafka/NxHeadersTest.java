package app.l2nx.gs.adapter.api.kafka;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NxHeadersTest {

    @Test
    void NX_SERVER_ID_shouldBeStableLiteral() {
        assertEquals("Nx-Server-Id", NxHeaders.NX_SERVER_ID);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "00000000-0000-0000-0000-000000000000",
            "ffffffff-ffff-ffff-ffff-ffffffffffff",
            "01234567-89ab-cdef-fedc-ba9876543210",
            "550e8400-e29b-41d4-a716-446655440000"
    })
    void encodeUuid_shouldRoundtripThroughDecodeUuid(String literal) {
        UUID uuid = UUID.fromString(literal);

        byte[] encoded = NxHeaders.encodeUuid(uuid);

        assertEquals(16, encoded.length);
        assertEquals(uuid, NxHeaders.decodeUuid(encoded));
    }

    @Test
    void encodeUuid_shouldProduceMostSigThenLeastSigBigEndian() {
        // UUID 01234567-89ab-cdef-fedc-ba9876543210
        // mostSig  = 0x0123456789abcdefL
        // leastSig = 0xfedcba9876543210L
        UUID uuid = UUID.fromString("01234567-89ab-cdef-fedc-ba9876543210");

        byte[] encoded = NxHeaders.encodeUuid(uuid);

        byte[] expected = new byte[]{
                (byte) 0x01, (byte) 0x23, (byte) 0x45, (byte) 0x67,
                (byte) 0x89, (byte) 0xab, (byte) 0xcd, (byte) 0xef,
                (byte) 0xfe, (byte) 0xdc, (byte) 0xba, (byte) 0x98,
                (byte) 0x76, (byte) 0x54, (byte) 0x32, (byte) 0x10
        };
        assertArrayEquals(expected, encoded);
    }

    @Test
    void encodeUuid_shouldThrowNPE_whenUuidIsNull() {
        assertThrows(NullPointerException.class, () -> NxHeaders.encodeUuid(null));
    }

    @Test
    void decodeUuid_shouldThrowIAE_whenValueIsNull() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> NxHeaders.decodeUuid(null));
        assertEquals("UUID header value must not be null", ex.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 1, 8, 15, 17, 32})
    void decodeUuid_shouldThrowIAE_whenLengthIsNot16(int len) {
        byte[] bad = new byte[len];

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> NxHeaders.decodeUuid(bad));
        assertEquals("UUID header value must be 16 bytes, got " + len, ex.getMessage());
    }
}
