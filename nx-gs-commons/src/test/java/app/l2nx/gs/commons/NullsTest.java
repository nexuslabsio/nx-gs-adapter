package app.l2nx.gs.commons;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class NullsTest {

    @Test
    void zeroToNull_int_shouldReturnNull_whenZero() {
        assertNull(Nulls.zeroToNull(0));
    }

    @Test
    void zeroToNull_long_shouldReturnNull_whenZero() {
        assertNull(Nulls.zeroToNull(0L));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 42, -1, Integer.MAX_VALUE, Integer.MIN_VALUE})
    void zeroToNull_int_shouldReturnValue_whenNonZero(int raw) {
        assertEquals(Integer.valueOf(raw), Nulls.zeroToNull(raw));
    }

    @ParameterizedTest
    @ValueSource(longs = {1L, 99L, -1L, Long.MAX_VALUE, Long.MIN_VALUE})
    void zeroToNull_long_shouldReturnValue_whenNonZero(long raw) {
        assertEquals(Long.valueOf(raw), Nulls.zeroToNull(raw));
    }

    @Test
    void zeroToNull_boxedInteger_shouldReturnNull_whenNullOrZero() {
        assertNull(Nulls.zeroToNull((Integer) null));
        assertNull(Nulls.zeroToNull(Integer.valueOf(0)));
    }

    @Test
    void zeroToNull_boxedLong_shouldReturnNull_whenNullOrZero() {
        assertNull(Nulls.zeroToNull((Long) null));
        assertNull(Nulls.zeroToNull(Long.valueOf(0L)));
    }

    @Test
    void zeroToNull_boxedInteger_shouldReturnValue_whenNonZero() {
        assertEquals(Integer.valueOf(7), Nulls.zeroToNull(Integer.valueOf(7)));
    }

    @Test
    void zeroToNull_boxedLong_shouldReturnValue_whenNonZero() {
        assertEquals(Long.valueOf(123L), Nulls.zeroToNull(Long.valueOf(123L)));
    }
}
