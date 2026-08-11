package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.adapter.api.domain.Attribute;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BuyLineTest {

    private static BuyLine.Builder valid() {
        return BuyLine.builder().itemId(1).itemTemplateId(57L).count(1L).unitPriceAdena(100L);
    }

    @ParameterizedTest(name = "itemId={0}")
    @CsvSource({"0", "-1"})
    void constructor_shouldReject_whenItemIdNotPositive(int itemId) {
        BuyLine.Builder builder = valid().itemId(itemId);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "itemTemplateId={0}")
    @CsvSource({"0", "-1"})
    void constructor_shouldReject_whenItemTemplateIdNotPositive(long itemTemplateId) {
        BuyLine.Builder builder = valid().itemTemplateId(itemTemplateId);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "count={0}")
    @CsvSource({"0", "-1"})
    void constructor_shouldReject_whenCountNotPositive(long count) {
        BuyLine.Builder builder = valid().count(count);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenUnitPriceAdenaNegative() {
        BuyLine.Builder builder = valid().unitPriceAdena(-1L);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenCountTimesUnitPriceOverflows() {
        BuyLine.Builder builder = valid().count(Long.MAX_VALUE).unitPriceAdena(2L);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "enchantLevel={0}")
    @CsvSource({"-1", "128"})
    void constructor_shouldReject_whenEnchantLevelOutOfRange(int enchantLevel) {
        BuyLine.Builder builder = valid().enchantLevel(enchantLevel);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "enchantLevel={0}")
    @CsvSource({"0", "127"})
    void constructor_shouldAccept_whenEnchantLevelAtBounds(int enchantLevel) {
        BuyLine line = valid().enchantLevel(enchantLevel).build();
        assertEquals(Integer.valueOf(enchantLevel), line.getEnchantLevel());
    }

    @Test
    void getEnchantLevel_shouldDefaultToNull() {
        BuyLine line = valid().build();
        assertNull(line.getEnchantLevel());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderOmits() {
        BuyLine line = valid().build();
        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldReturnEmptyMap_whenBuilderPassesNull() {
        BuyLine line = valid().attributes(null).build();
        assertTrue(line.getAttributes().isEmpty());
    }

    @Test
    void getAttributes_shouldBeUnmodifiable() {
        Map<Attribute, Integer> source = new HashMap<>();
        source.put(Attribute.FIRE, 300);
        BuyLine line = valid().attributes(source).build();

        assertThrows(
                UnsupportedOperationException.class, () -> line.getAttributes().put(Attribute.WATER, 150));
    }

    @Test
    void constructor_shouldDefensivelyCopyAttributes() {
        Map<Attribute, Integer> source = new HashMap<>();
        source.put(Attribute.FIRE, 300);
        BuyLine line = valid().attributes(source).build();

        source.put(Attribute.WATER, 150);

        assertEquals(1, line.getAttributes().size());
        assertEquals(Integer.valueOf(300), line.getAttributes().get(Attribute.FIRE));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        Map<Attribute, Integer> attrs = new LinkedHashMap<>();
        attrs.put(Attribute.FIRE, 300);
        attrs.put(Attribute.HOLY, 150);

        BuyLine original = BuyLine.builder()
                .itemId(268437521)
                .itemTemplateId(57L)
                .enchantLevel(16)
                .attributes(attrs)
                .count(3L)
                .unitPriceAdena(1_500_000L)
                .build();

        BuyLine copy = original.toBuilder().build();
        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishItemId() {
        BuyLine a = valid().itemId(1).build();
        BuyLine b = valid().itemId(2).build();
        assertTrue(!a.equals(b));
        assertTrue(a.hashCode() != b.hashCode() || !a.equals(b));
    }
}
