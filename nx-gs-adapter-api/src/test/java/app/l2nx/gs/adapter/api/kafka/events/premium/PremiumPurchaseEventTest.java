package app.l2nx.gs.adapter.api.kafka.events.premium;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class PremiumPurchaseEventTest {

    @Test
    void getItems_shouldReturnEmptyList_whenBuilderOmits() {
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .build();

        assertTrue(event.getItems().isEmpty());
    }

    @Test
    void getServices_shouldReturnEmptyList_whenBuilderOmits() {
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .build();

        assertTrue(event.getServices().isEmpty());
    }

    @Test
    void getCharacterName_shouldBeNullable() {
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .build();

        assertNull(event.getCharacterName());
        assertNull(event.getAccountName());
    }

    @Test
    void getItems_shouldBeUnmodifiable() {
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .items(Collections.singletonList(
                        PurchaseItem.builder().itemId(9627).qty(1)
                                .payments(Collections.singletonList(
                                        Payment.builder().currencyItemId(4037).qty(20).build()))
                                .build()))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getItems().add(null));
    }

    @Test
    void getServices_shouldBeUnmodifiable() {
        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .services(Collections.singletonList(
                        PurchaseService.builder().code(WellKnownServices.NOBLESSE)
                                .payments(Collections.singletonList(
                                        Payment.builder().currencyItemId(4037).qty(50).build()))
                                .build()))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> event.getServices().add(null));
    }

    @Test
    void constructor_shouldDefensivelyCopyItemsList() {
        List<PurchaseItem> source = new ArrayList<PurchaseItem>();
        source.add(PurchaseItem.builder().itemId(1).qty(1)
                .payments(Collections.singletonList(Payment.builder().currencyItemId(57).qty(100).build()))
                .build());

        PremiumPurchaseEvent event = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(42L)
                .items(source)
                .build();

        source.add(PurchaseItem.builder().itemId(2).qty(2)
                .payments(Collections.singletonList(Payment.builder().currencyItemId(57).qty(100).build()))
                .build());

        assertEquals(1, event.getItems().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        Map<String, String> serviceParams = new HashMap<String, String>();
        serviceParams.put("rgb", "0xFFCC00");

        PremiumPurchaseEvent original = PremiumPurchaseEvent.builder()
                .eventId(UUID.randomUUID())
                .characterId(268437521L)
                .characterName("Hisho")
                .accountName("shanon")
                .items(Collections.singletonList(
                        PurchaseItem.builder().itemId(9627).qty(1)
                                .params(Collections.singletonMap("enchant", "3"))
                                .payments(Collections.singletonList(
                                        Payment.builder().currencyItemId(4037).qty(20).build()))
                                .build()))
                .services(Collections.singletonList(
                        PurchaseService.builder().code(WellKnownServices.NAME_COLOR_CHANGE)
                                .params(serviceParams)
                                .payments(Collections.singletonList(
                                        Payment.builder().currencyItemId(4037).qty(50).build()))
                                .build()))
                .build();

        assertEquals(original, original.toBuilder().build());
    }

    @Test
    void purchaseItem_getParams_shouldBeEmpty_whenNull() {
        PurchaseItem item = PurchaseItem.builder()
                .itemId(1).qty(1)
                .payments(Collections.singletonList(Payment.builder().currencyItemId(57).qty(100).build()))
                .build();

        assertTrue(item.getParams().isEmpty());
    }

    @Test
    void purchaseService_getParams_shouldBeEmpty_whenNull() {
        PurchaseService service = PurchaseService.builder()
                .code(WellKnownServices.NOBLESSE)
                .payments(Collections.singletonList(Payment.builder().currencyItemId(4037).qty(50).build()))
                .build();

        assertTrue(service.getParams().isEmpty());
    }

    @Test
    void payment_equals_shouldDistinguishCurrencyItemId() {
        Payment a = Payment.builder().currencyItemId(57).qty(100).build();
        Payment b = Payment.builder().currencyItemId(4037).qty(100).build();

        assertEquals(a, a.toBuilder().build());
        assertThrows(AssertionError.class, () -> assertEquals(a, b));
    }

    @Test
    void wellKnownServices_constantsShouldUseSnakeCase() {
        assertEquals("noblesse", WellKnownServices.NOBLESSE);
        assertEquals("name_color_change", WellKnownServices.NAME_COLOR_CHANGE);
        assertEquals("clan_lvl_up", WellKnownServices.CLAN_LVL_UP);
    }
}
