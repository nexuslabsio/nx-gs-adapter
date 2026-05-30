package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class CharacterRuntimeDtoTest {

    private static CustomActivity fishing() {
        return CustomActivity.builder()
                .type(WellKnownCustomActivities.FISHING)
                .metadata(Collections.singletonMap(
                        WellKnownCustomActivityMetadata.ELAPSED_SECONDS, "1820"))
                .build();
    }

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
                1000, 2000, 500, 600, -700, Boolean.TRUE, "attack", fishing());

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void builder_shouldCarryActivityFields() {
        CustomActivity activity = fishing();
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(42L)
                .aiStatus("idle")
                .customActivity(activity)
                .build();

        assertEquals("idle", dto.getAiStatus());
        assertEquals(activity, dto.getCustomActivity());
        assertEquals("fishing", dto.getCustomActivity().getType());
        assertEquals("1820", dto.getCustomActivity().getMetadata()
                .get(WellKnownCustomActivityMetadata.ELAPSED_SECONDS));
    }

    @Test
    void activityFields_shouldBeNullByDefault() {
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder().id(1L).build();

        assertNull(dto.getAiStatus());
        assertNull(dto.getCustomActivity());
    }

    @Test
    void activityFields_shouldBeIndependentInEquals() {
        CharacterRuntimeDto fishingIdle = CharacterRuntimeDto.builder()
                .id(1L).aiStatus("idle").customActivity(fishing()).build();
        CharacterRuntimeDto plainIdle = CharacterRuntimeDto.builder()
                .id(1L).aiStatus("idle").build();

        // customActivity differs (fishing vs none) though aiStatus matches —
        // the two fields are orthogonal and both participate in equality.
        assertNotEquals(fishingIdle, plainIdle);
    }

    @Test
    void toBuilder_shouldRoundtripActivityFields() {
        CharacterRuntimeDto original = CharacterRuntimeDto.builder()
                .id(9L).aiStatus("cast").customActivity(fishing()).build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterRuntimeDto fromBuilder = CharacterRuntimeDto.builder().id(7L).build();
        CharacterRuntimeDto fromCtor = new CharacterRuntimeDto(
                7L, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }
}
