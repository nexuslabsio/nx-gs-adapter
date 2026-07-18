package app.l2nx.gs.adapter.api.kafka.sync.db.item;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ItemAugmentationDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        ItemAugmentationDbDto augmentation =
                ItemAugmentationDbDto.builder().option1Id(1000).option2Id(2000).build();

        assertEquals(1000, augmentation.getOption1Id());
        assertEquals(Integer.valueOf(2000), augmentation.getOption2Id());
    }

    @Test
    void option2Id_shouldBeNullable_whenItemHasSingleOption() {
        ItemAugmentationDbDto augmentation =
                ItemAugmentationDbDto.builder().option1Id(1000).build();

        assertEquals(1000, augmentation.getOption1Id());
        assertNull(augmentation.getOption2Id());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects() {
        ItemAugmentationDbDto fromBuilder =
                ItemAugmentationDbDto.builder().option1Id(1000).option2Id(2000).build();
        ItemAugmentationDbDto fromCtor = new ItemAugmentationDbDto(1000, 2000);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void equals_shouldDiffer_whenOption2IdDiffers() {
        ItemAugmentationDbDto singleOption =
                ItemAugmentationDbDto.builder().option1Id(1000).build();
        ItemAugmentationDbDto dualOption =
                ItemAugmentationDbDto.builder().option1Id(1000).option2Id(2000).build();

        assertNotEquals(singleOption, dualOption);
        assertNotEquals(singleOption.hashCode(), dualOption.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        ItemAugmentationDbDto original = new ItemAugmentationDbDto(1000, 2000);

        assertEquals(original, original.toBuilder().build());
    }
}
