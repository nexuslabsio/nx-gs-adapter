package app.l2nx.gs.adapter.api.kafka.sync.db;

import app.l2nx.gs.adapter.api.domain.Race;
import app.l2nx.gs.adapter.api.domain.Sex;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CharacterDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        CharacterDto ch = CharacterDto.builder()
                .id(54321L)
                .name("Cliodhna")
                .title("Hellbound")
                .level(85)
                .sex(Sex.FEMALE)
                .race(Race.ELF)
                .clanId(909L)
                .pvpCounter(1200)
                .pkCounter(7)
                .karma(0)
                .build();

        assertEquals(54321L, ch.getId());
        assertEquals("Cliodhna", ch.getName());
        assertEquals("Hellbound", ch.getTitle());
        assertEquals(Integer.valueOf(85), ch.getLevel());
        assertEquals(Sex.FEMALE, ch.getSex());
        assertEquals(Race.ELF, ch.getRace());
        assertEquals(Long.valueOf(909L), ch.getClanId());
        assertEquals(Integer.valueOf(1200), ch.getPvpCounter());
        assertEquals(Integer.valueOf(7), ch.getPkCounter());
        assertEquals(Integer.valueOf(0), ch.getKarma());
    }

    @Test
    void allOptionalFields_shouldBeNullable_whenTenantOmitsThem() {
        CharacterDto ch = CharacterDto.builder().id(1L).build();

        assertEquals(1L, ch.getId());
        assertNull(ch.getName());
        assertNull(ch.getTitle());
        assertNull(ch.getLevel());
        assertNull(ch.getSex());
        assertNull(ch.getRace());
        assertNull(ch.getClanId());
        assertNull(ch.getPvpCounter());
        assertNull(ch.getPkCounter());
        assertNull(ch.getKarma());
    }

    @Test
    void clanId_shouldBeNullable_forSentinelZeroSourceValue() {
        CharacterDto ch = CharacterDto.builder().id(1L).clanId(null).build();

        assertNull(ch.getClanId());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterDto fromBuilder = CharacterDto.builder().id(1L).build();
        CharacterDto fromCtor = new CharacterDto(1L, null, null, null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterDto original = new CharacterDto(1L, "X", "", 10,
                Sex.MALE, Race.HUMAN, null, 0, 0, 0);

        assertEquals(original, original.toBuilder().build());
    }
}
