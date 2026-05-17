package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CharacterRuntimeDtoTest {

    @Test
    void builder_shouldPopulateOnlineLiveStateRow() {
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(42L)
                .curHp(1234)
                .maxHp(2000)
                .x(100)
                .y(200)
                .z(-3000)
                .build();

        assertEquals(42L, dto.getId());
        assertEquals(Integer.valueOf(1234), dto.getCurHp());
        assertEquals(Integer.valueOf(2000), dto.getMaxHp());
        assertNull(dto.getOnline());
    }

    @Test
    void builder_shouldPopulateOfflineTombstone() {
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(42L)
                .online(Boolean.FALSE)
                .build();

        assertEquals(42L, dto.getId());
        assertEquals(Boolean.FALSE, dto.getOnline());
        assertNull(dto.getCurHp());
        assertNull(dto.getMaxHp());
        assertNull(dto.getX());
    }

    @Test
    void onlineNullAndTrue_shouldNotBeEqual_perFieldSemantics() {
        CharacterRuntimeDto omittedOnline = CharacterRuntimeDto.builder().id(1L).build();
        CharacterRuntimeDto explicitTrue = CharacterRuntimeDto.builder().id(1L).online(Boolean.TRUE).build();

        // Consumer-side semantics treat null and true alike, but the DTO itself
        // preserves the distinction — equals reflects raw field state.
        assertNotEquals(omittedOnline, explicitTrue);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterRuntimeDto original = new CharacterRuntimeDto(
                123L, 100, 200, 50, 100, 25, 50,
                1000, 2000, 500, 600, -700, Boolean.TRUE);

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterRuntimeDto fromBuilder = CharacterRuntimeDto.builder().id(7L).build();
        CharacterRuntimeDto fromCtor = new CharacterRuntimeDto(
                7L, null, null, null, null, null, null,
                null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }
}
