package app.l2nx.gs.adapter.api.kafka.commands.privatestore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class BuyFromPrivateStoreCommandTest {

    private static final Instant DEADLINE = Instant.parse("2026-08-11T12:00:00Z");

    private static BuyLine line(int itemId) {
        return BuyLine.builder()
                .itemId(itemId)
                .itemTemplateId(57L)
                .count(1L)
                .unitPriceAdena(100L)
                .build();
    }

    private static BuyFromPrivateStoreCommand.Builder valid() {
        return BuyFromPrivateStoreCommand.builder()
                .buyerCharId(1)
                .sellerCharId(2)
                .lines(Collections.singletonList(line(1)))
                .tax(5)
                .deadline(DEADLINE);
    }

    @ParameterizedTest(name = "buyerCharId={0}")
    @CsvSource({"0", "-1"})
    void constructor_shouldReject_whenBuyerCharIdNotPositive(int buyerCharId) {
        BuyFromPrivateStoreCommand.Builder builder = valid().buyerCharId(buyerCharId);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "sellerCharId={0}")
    @CsvSource({"0", "-1"})
    void constructor_shouldReject_whenSellerCharIdNotPositive(int sellerCharId) {
        BuyFromPrivateStoreCommand.Builder builder = valid().sellerCharId(sellerCharId);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenBuyerEqualsSeller() {
        BuyFromPrivateStoreCommand.Builder builder = valid().buyerCharId(5).sellerCharId(5);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenLinesNull() {
        BuyFromPrivateStoreCommand.Builder builder = valid().lines(null);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenLinesEmpty() {
        BuyFromPrivateStoreCommand.Builder builder = valid().lines(Collections.emptyList());
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenLinesContainNull() {
        List<BuyLine> lines = new ArrayList<>();
        lines.add(line(1));
        lines.add(null);
        BuyFromPrivateStoreCommand.Builder builder = valid().lines(lines);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldReject_whenLinesExceedMaxLines() {
        List<BuyLine> lines = new ArrayList<>();
        for (int i = 1; i <= BuyFromPrivateStoreCommand.MAX_LINES + 1; i++) {
            lines.add(line(i));
        }
        BuyFromPrivateStoreCommand.Builder builder = valid().lines(lines);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void constructor_shouldAccept_whenLinesAtMaxLines() {
        List<BuyLine> lines = new ArrayList<>();
        for (int i = 1; i <= BuyFromPrivateStoreCommand.MAX_LINES; i++) {
            lines.add(line(i));
        }
        BuyFromPrivateStoreCommand command = valid().lines(lines).build();
        assertEquals(BuyFromPrivateStoreCommand.MAX_LINES, command.getLines().size());
    }

    @Test
    void constructor_shouldReject_whenItemIdDuplicatedAcrossLines() {
        List<BuyLine> lines = Arrays.asList(line(1), line(1));
        BuyFromPrivateStoreCommand.Builder builder = valid().lines(lines);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "tax={0}")
    @CsvSource({"-1", "51"})
    void constructor_shouldReject_whenTaxOutOfRange(int tax) {
        BuyFromPrivateStoreCommand.Builder builder = valid().tax(tax);
        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @ParameterizedTest(name = "tax={0}")
    @CsvSource({"0", "50"})
    void constructor_shouldAccept_whenTaxAtBounds(int tax) {
        BuyFromPrivateStoreCommand command = valid().tax(tax).build();
        assertEquals(tax, command.getTax());
    }

    @Test
    void constructor_shouldReject_whenDeadlineNull() {
        BuyFromPrivateStoreCommand.Builder builder = valid().deadline(null);
        assertThrows(NullPointerException.class, builder::build);
    }

    @Test
    void getLines_shouldBeUnmodifiable() {
        BuyFromPrivateStoreCommand command = valid().build();
        assertThrows(
                UnsupportedOperationException.class, () -> command.getLines().add(line(2)));
    }

    @Test
    void toBuilder_shouldRoundtripAllFields() {
        BuyFromPrivateStoreCommand original = valid().build();

        BuyFromPrivateStoreCommand copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertNotSame(original, copy);
    }

    @Test
    void equals_shouldDistinguishDeadline() {
        BuyFromPrivateStoreCommand a = valid().deadline(DEADLINE).build();
        BuyFromPrivateStoreCommand b = valid().deadline(DEADLINE.plusSeconds(1)).build();
        assertTrue(!a.equals(b));
    }
}
