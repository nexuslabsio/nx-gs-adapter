package app.l2nx.gs.adapter.api.kafka.sync.db.item;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.domain.Attribute;
import app.l2nx.gs.adapter.api.domain.item.ItemLocation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class ItemDbDtoTest {

    @Test
    void builder_shouldMapEachFieldToConstructorPosition() {
        List<ItemAttributeDbDto> attrs = Arrays.asList(
                ItemAttributeDbDto.builder().type(Attribute.FIRE).value(150).build(),
                ItemAttributeDbDto.builder().type(Attribute.WATER).value(60).build());
        ItemAugmentationDbDto augmentation =
                ItemAugmentationDbDto.builder().option1Id(1000).option2Id(2000).build();

        ItemDbDto item = ItemDbDto.builder()
                .id(98765L)
                .itemTemplateId(7575L)
                .ownerId(12345L)
                .count(1L)
                .enchantLevel(16)
                .location(ItemLocation.EQUIP)
                .attributes(attrs)
                .augmentation(augmentation)
                .build();

        assertEquals(98765L, item.getId());
        assertEquals(Long.valueOf(7575L), item.getItemTemplateId());
        assertEquals(Long.valueOf(12345L), item.getOwnerId());
        assertEquals(Long.valueOf(1L), item.getCount());
        assertEquals(Integer.valueOf(16), item.getEnchantLevel());
        assertEquals(ItemLocation.EQUIP, item.getLocation());
        assertEquals(attrs, item.getAttributes());
        assertEquals(augmentation, item.getAugmentation());
    }

    @Test
    void allOptionalFields_shouldBeNullable_whenTenantOmitsThem() {
        ItemDbDto item = ItemDbDto.builder().id(1L).build();

        assertEquals(1L, item.getId());
        assertNull(item.getItemTemplateId());
        assertNull(item.getOwnerId());
        assertNull(item.getCount());
        assertNull(item.getEnchantLevel());
        assertNull(item.getLocation());
        assertNull(item.getAttributes());
        assertNull(item.getAugmentation());
    }

    @Test
    void ownerId_shouldBeNullable_forSentinelZeroSourceValue() {
        ItemDbDto item = ItemDbDto.builder().id(1L).ownerId(null).build();

        assertNull(item.getOwnerId());
    }

    @Test
    void attributes_shouldBeNull_whenTenantDoesNotSyncThem() {
        ItemDbDto item = ItemDbDto.builder().id(1L).build();

        assertNull(item.getAttributes());
    }

    @Test
    void attributes_shouldBeEmptyList_whenTenantSyncsThemButItemHasNone() {
        ItemDbDto item =
                ItemDbDto.builder().id(1L).attributes(Collections.emptyList()).build();

        assertNotNull(item.getAttributes());
        assertTrue(item.getAttributes().isEmpty());
    }

    @Test
    void builder_andConstructor_shouldProduceEqualObjects_whenAllOptionalNull() {
        ItemDbDto fromBuilder = ItemDbDto.builder().id(1L).build();
        ItemDbDto fromCtor = new ItemDbDto(1L, null, null, null, null, null, null, null);

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        List<ItemAttributeDbDto> attrs = Collections.singletonList(
                ItemAttributeDbDto.builder().type(Attribute.HOLY).value(120).build());
        ItemAugmentationDbDto augmentation =
                ItemAugmentationDbDto.builder().option1Id(1000).option2Id(2000).build();
        ItemDbDto original = new ItemDbDto(1L, 7L, 2L, 5L, 10, ItemLocation.INVENTORY, attrs, augmentation);

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void augmentation_shouldBeNull_whenTenantDoesNotSyncIt() {
        ItemDbDto item = ItemDbDto.builder().id(1L).build();

        assertNull(item.getAugmentation());
    }

    @Test
    void equals_shouldDiffer_whenAugmentationDiffers() {
        ItemDbDto withAugmentation = ItemDbDto.builder()
                .id(1L)
                .augmentation(ItemAugmentationDbDto.builder()
                        .option1Id(1000)
                        .option2Id(2000)
                        .build())
                .build();
        ItemDbDto withoutAugmentation = ItemDbDto.builder().id(1L).build();

        assertNotEquals(withAugmentation, withoutAugmentation);
        assertNotEquals(withAugmentation.hashCode(), withoutAugmentation.hashCode());
    }
}
