package app.l2nx.gs.adapter.api.kafka.sync.db.alliance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AllianceDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        byte[] icon = new byte[]{0x11, 0x22, 0x33};
        AllianceDbDto alliance = AllianceDbDto.builder()
                .id(42L)
                .name("Crusaders")
                .icon(icon)
                .build();

        assertEquals(42L, alliance.getId());
        assertEquals("Crusaders", alliance.getName());
        assertArrayEquals(icon, alliance.getIcon());
    }

    @Test
    void icon_shouldBeNull_whenTenantDoesNotSyncCrests() {
        AllianceDbDto alliance = AllianceDbDto.builder()
                .id(1L).name("X").build();
        assertNull(alliance.getIcon());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        AllianceDbDto original = AllianceDbDto.builder()
                .id(7L).name("RoundTrip").icon(new byte[]{9}).build();
        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldCompareIconBytes() {
        AllianceDbDto a = AllianceDbDto.builder().id(1L).name("X").icon(new byte[]{1}).build();
        AllianceDbDto b = AllianceDbDto.builder().id(1L).name("X").icon(new byte[]{1}).build();
        AllianceDbDto c = AllianceDbDto.builder().id(1L).name("X").icon(new byte[]{2}).build();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }

    @Test
    void build_shouldThrowNpe_whenNameIsNull() {
        AllianceDbDto.Builder b = AllianceDbDto.builder().id(1L);

        assertThrows(NullPointerException.class, b::build);
    }
}
