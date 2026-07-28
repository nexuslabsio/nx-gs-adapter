package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class CharacterRuntimeDtoTest {

    private static Activity fishing() {
        return Activity.builder()
                .type(WellKnownActivities.FISHING)
                .metadata(Collections.singletonMap(WellKnownActivityMetadata.ELAPSED_SECONDS, "1820"))
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
        CharacterRuntimeDto dto =
                CharacterRuntimeDto.builder().id(42L).online(Boolean.FALSE).build();

        assertEquals(42L, dto.getId());
        assertEquals(Boolean.FALSE, dto.getOnline());
        assertNull(dto.getCurHp());
        assertNull(dto.getMaxHp());
        assertNull(dto.getX());
    }

    @Test
    void onlineNullAndTrue_shouldNotBeEqual_perFieldSemantics() {
        CharacterRuntimeDto omittedOnline = CharacterRuntimeDto.builder().id(1L).build();
        CharacterRuntimeDto explicitTrue =
                CharacterRuntimeDto.builder().id(1L).online(Boolean.TRUE).build();

        // Consumer-side semantics treat null and true alike, but the DTO itself
        // preserves the distinction — equals reflects raw field state.
        assertNotEquals(omittedOnline, explicitTrue);
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        CharacterRuntimeDto original = new CharacterRuntimeDto(
                123L,
                100,
                200,
                50,
                100,
                25,
                50,
                1000,
                2000,
                500,
                600,
                -700,
                Boolean.TRUE,
                "attack",
                CharacterClass.SOULTAKER,
                76,
                4_500_000_000L,
                12_345L,
                Collections.singletonList(fishing()),
                80,
                100,
                12,
                50,
                45000,
                60000,
                null);

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void builder_shouldCarryActivityFields() {
        Activity activity = fishing();
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(42L)
                .aiStatus("idle")
                .activities(Collections.singletonList(activity))
                .build();

        assertEquals("idle", dto.getAiStatus());
        assertEquals(Collections.singletonList(activity), dto.getActivities());
        assertEquals("fishing", dto.getActivities().get(0).getType());
        assertEquals("1820", dto.getActivities().get(0).getMetadata().get(WellKnownActivityMetadata.ELAPSED_SECONDS));
    }

    @Test
    @SuppressWarnings("deprecation")
    void getActivities_shouldFallBackToLegacyWireName_whenHostStillEmitsCustomActivities() {
        Activity activity = fishing();
        // Positional construction mirrors what a name-binding deserializer does with
        // JSON from a host that has not been restarted onto the renamed contract.
        CharacterRuntimeDto dto = new CharacterRuntimeDto(
                42L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(activity));

        assertEquals(Collections.singletonList(activity), dto.getActivities());
    }

    @Test
    @SuppressWarnings("deprecation")
    void getActivities_shouldPreferNewWireName_whenBothArePresent() {
        Activity autofarming =
                Activity.builder().type(WellKnownActivities.AUTOFARMING).build();
        CharacterRuntimeDto dto = new CharacterRuntimeDto(
                42L,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(autofarming),
                null,
                null,
                null,
                null,
                null,
                null,
                Collections.singletonList(fishing()));

        assertEquals(Collections.singletonList(autofarming), dto.getActivities());
    }

    @Test
    void activities_shouldCarryMultipleEntries() {
        Activity autofarming = Activity.builder()
                .type(WellKnownActivities.AUTOFARMING)
                .metadata(Collections.singletonMap(WellKnownActivityMetadata.SECONDS_REMAINING, "3600"))
                .build();
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(42L)
                .activities(Arrays.asList(fishing(), autofarming))
                .build();

        assertEquals(2, dto.getActivities().size());
        assertEquals("fishing", dto.getActivities().get(0).getType());
        assertEquals("autofarming", dto.getActivities().get(1).getType());
    }

    @Test
    void activities_shouldBeUnmodifiableAndDefensivelyCopied() {
        List<Activity> source = new ArrayList<>();
        source.add(fishing());
        CharacterRuntimeDto dto =
                CharacterRuntimeDto.builder().id(1L).activities(source).build();

        source.clear();
        assertEquals(1, dto.getActivities().size());
        assertThrows(
                UnsupportedOperationException.class, () -> dto.getActivities().add(fishing()));
    }

    @Test
    void activityFields_shouldBeNullByDefault() {
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder().id(1L).build();

        assertNull(dto.getAiStatus());
        assertNull(dto.getActivities());
    }

    @Test
    void activityFields_shouldBeIndependentInEquals() {
        CharacterRuntimeDto fishingIdle = CharacterRuntimeDto.builder()
                .id(1L)
                .aiStatus("idle")
                .activities(Collections.singletonList(fishing()))
                .build();
        CharacterRuntimeDto plainIdle =
                CharacterRuntimeDto.builder().id(1L).aiStatus("idle").build();

        // activities differ (fishing vs none) though aiStatus matches —
        // the two fields are orthogonal and both participate in equality.
        assertNotEquals(fishingIdle, plainIdle);
    }

    @Test
    void toBuilder_shouldRoundtripActivityFields() {
        CharacterRuntimeDto original = CharacterRuntimeDto.builder()
                .id(9L)
                .aiStatus("cast")
                .activities(Collections.singletonList(fishing()))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        CharacterRuntimeDto fromBuilder = CharacterRuntimeDto.builder().id(7L).build();
        CharacterRuntimeDto fromCtor = new CharacterRuntimeDto(
                7L, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void activeClassFields_shouldBeNullByDefaultAndRoundtrip() {
        CharacterRuntimeDto empty = CharacterRuntimeDto.builder().id(1L).build();
        assertNull(empty.getClassId());
        assertNull(empty.getLevel());
        assertNull(empty.getSp());

        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(1L)
                .classId(CharacterClass.SOULTAKER)
                .level(76)
                .exp(4_500_000_000L)
                .sp(12_345L)
                .build();

        assertEquals(CharacterClass.SOULTAKER, dto.getClassId());
        assertEquals(Integer.valueOf(76), dto.getLevel());
        assertEquals(Long.valueOf(12_345L), dto.getSp());
        assertEquals(dto, dto.toBuilder().build());
        assertNotEquals(dto, empty);
    }

    @Test
    void exp_shouldBeNullByDefaultAndRoundtrip() {
        assertNull(CharacterRuntimeDto.builder().id(1L).build().getExp());

        CharacterRuntimeDto withExp =
                CharacterRuntimeDto.builder().id(1L).exp(4_500_000_000L).build();
        assertEquals(Long.valueOf(4_500_000_000L), withExp.getExp());
        assertEquals(withExp, withExp.toBuilder().build());

        CharacterRuntimeDto noExp = CharacterRuntimeDto.builder().id(1L).build();
        assertNotEquals(withExp, noExp);
    }

    @Test
    void builder_shouldRoundtripInventoryCapacityFields() {
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder()
                .id(1L)
                .curInventorySlots(80)
                .maxInventorySlots(100)
                .curQuestInventorySlots(12)
                .maxQuestInventorySlots(50)
                .curWeight(45000)
                .maxWeight(60000)
                .build();

        assertEquals(Integer.valueOf(80), dto.getCurInventorySlots());
        assertEquals(Integer.valueOf(100), dto.getMaxInventorySlots());
        assertEquals(Integer.valueOf(12), dto.getCurQuestInventorySlots());
        assertEquals(Integer.valueOf(50), dto.getMaxQuestInventorySlots());
        assertEquals(Integer.valueOf(45000), dto.getCurWeight());
        assertEquals(Integer.valueOf(60000), dto.getMaxWeight());
        assertEquals(dto, dto.toBuilder().build());
    }

    @Test
    void inventoryCapacityFields_shouldDefaultToNull() {
        CharacterRuntimeDto dto = CharacterRuntimeDto.builder().id(123L).build();

        assertNull(dto.getCurInventorySlots());
        assertNull(dto.getMaxInventorySlots());
        assertNull(dto.getCurQuestInventorySlots());
        assertNull(dto.getMaxQuestInventorySlots());
        assertNull(dto.getCurWeight());
        assertNull(dto.getMaxWeight());
    }

    /**
     * The DTO carries no binder annotations, so consumers bind it through implicit
     * constructor-parameter names — which only resolves while exactly one constructor is visible.
     * A second one (e.g. a back-compat overload when the wire grows) makes creator detection
     * ambiguous and every consumer silently fails to deserialize the whole channel.
     */
    @Test
    void class_shouldExposeExactlyOneConstructor() {
        assertEquals(1, CharacterRuntimeDto.class.getDeclaredConstructors().length);
    }

    @Test
    void equalsAndHashCode_shouldDifferWhenInventoryCapacityFieldDiffers() {
        CharacterRuntimeDto base = CharacterRuntimeDto.builder()
                .id(1L)
                .curInventorySlots(80)
                .maxInventorySlots(100)
                .curQuestInventorySlots(12)
                .maxQuestInventorySlots(50)
                .curWeight(45000)
                .maxWeight(60000)
                .build();

        assertNotEquals(base, base.toBuilder().curInventorySlots(81).build());
        assertNotEquals(base, base.toBuilder().maxInventorySlots(101).build());
        assertNotEquals(base, base.toBuilder().curQuestInventorySlots(13).build());
        assertNotEquals(base, base.toBuilder().maxQuestInventorySlots(51).build());
        assertNotEquals(base, base.toBuilder().curWeight(45001).build());
        assertNotEquals(base, base.toBuilder().maxWeight(60001).build());

        CharacterRuntimeDto sameValues = base.toBuilder().build();
        assertEquals(base, sameValues);
        assertEquals(base.hashCode(), sameValues.hashCode());
    }
}
