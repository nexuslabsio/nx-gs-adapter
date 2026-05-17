package app.l2nx.gs.adapter.api.kafka.sync.db.alliance;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class AllianceDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        byte[] icon = new byte[]{0x11, 0x22, 0x33};
        AllianceDto alliance = AllianceDto.builder()
                .allyId(42L)
                .allyName("Crusaders")
                .icon(icon)
                .build();

        assertEquals(42L, alliance.getAllyId());
        assertEquals("Crusaders", alliance.getAllyName());
        assertArrayEquals(icon, alliance.getIcon());
    }

    @Test
    void icon_shouldBeNull_whenTenantDoesNotSyncCrests() {
        AllianceDto alliance = AllianceDto.builder()
                .allyId(1L).allyName("X").build();
        assertNull(alliance.getIcon());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        AllianceDto original = AllianceDto.builder()
                .allyId(7L).allyName("RoundTrip").icon(new byte[]{9}).build();
        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void equals_shouldCompareIconBytes() {
        AllianceDto a = AllianceDto.builder().allyId(1L).allyName("X").icon(new byte[]{1}).build();
        AllianceDto b = AllianceDto.builder().allyId(1L).allyName("X").icon(new byte[]{1}).build();
        AllianceDto c = AllianceDto.builder().allyId(1L).allyName("X").icon(new byte[]{2}).build();
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
