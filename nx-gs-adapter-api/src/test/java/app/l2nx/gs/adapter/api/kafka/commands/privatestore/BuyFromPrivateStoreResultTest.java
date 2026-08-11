package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BuyFromPrivateStoreResultTest {

    private static BoughtLine boughtLine() {
        return BoughtLine.builder()
                .itemId(1)
                .itemTemplateId(57L)
                .count(1L)
                .unitPriceAdena(100L)
                .build();
    }

    private static BuyFromPrivateStoreResult.Builder valid() {
        return BuyFromPrivateStoreResult.builder()
                .itemsTotalAdena(100L)
                .taxAdena(5L)
                .paidTotalAdena(105L)
                .bought(Collections.singletonList(boughtLine()))
                .storeClosed(false)
                .mailId(42L);
    }

    @ParameterizedTest(name = "items={0}, tax={1}, paid={2}")
    @CsvSource({"100, 5, 104", "100, 5, 106", "0, 0, 1"})
    void constructor_shouldReject_whenPaidTotalDoesNotEqualItemsPlusTax(long items, long tax, long paid) {
        BuyFromPrivateStoreResult.Builder builder =
                valid().itemsTotalAdena(items).taxAdena(tax).paidTotalAdena(paid);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "items={0}, tax={1}")
    @CsvSource({"100, 5", "0, 0", "1000000, 0"})
    void constructor_shouldAccept_whenPaidTotalEqualsItemsPlusTax(long items, long tax) {
        BuyFromPrivateStoreResult result = valid().itemsTotalAdena(items)
                .taxAdena(tax)
                .paidTotalAdena(items + tax)
                .build();
        assertEquals(items + tax, result.getPaidTotalAdena());
    }

    @Test
    void getBought_shouldReturnEmptyList_whenBuilderOmits() {
        BuyFromPrivateStoreResult result = BuyFromPrivateStoreResult.builder()
                .itemsTotalAdena(0L)
                .taxAdena(0L)
                .paidTotalAdena(0L)
                .storeClosed(false)
                .mailId(0L)
                .build();

        assertTrue(result.getBought().isEmpty());
    }

    @Test
    void getBought_shouldBeUnmodifiable() {
        BuyFromPrivateStoreResult result = valid().build();
        List<BoughtLine> bought = result.getBought();
        assertThrows(UnsupportedOperationException.class, () -> bought.add(boughtLine()));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        BuyFromPrivateStoreResult original = valid().build();

        BuyFromPrivateStoreResult copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void isStoreClosed_shouldReflectBuilderValue() {
        BuyFromPrivateStoreResult result = valid().storeClosed(true).build();
        assertTrue(result.isStoreClosed());
    }
}
