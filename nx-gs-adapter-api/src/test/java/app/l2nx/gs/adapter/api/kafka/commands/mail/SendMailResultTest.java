package app.l2nx.gs.adapter.api.kafka.commands.mail;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;

class SendMailResultTest {

    @Test
    void builder_shouldRoundtripFields() {
        ItemDeliveryError err = ItemDeliveryError.builder()
                .itemTemplateId(57L)
                .count(1L)
                .reason("x")
                .build();

        SendMailResult result = SendMailResult.builder()
                .createdMailIds(Arrays.asList(101L, 102L))
                .itemErrors(Collections.singletonList(err))
                .build();

        assertEquals(Arrays.asList(101L, 102L), result.getCreatedMailIds());
        assertEquals(Collections.singletonList(err), result.getItemErrors());
    }

    @Test
    void getCreatedMailIds_shouldReturnEmpty_whenNull() {
        SendMailResult result = new SendMailResult(null, null);

        assertTrue(result.getCreatedMailIds().isEmpty());
    }

    @Test
    void getItemErrors_shouldReturnEmpty_whenNull() {
        SendMailResult result = new SendMailResult(null, null);

        assertTrue(result.getItemErrors().isEmpty());
    }

    @Test
    void getCreatedMailIds_shouldBeUnmodifiable() {
        SendMailResult result =
                SendMailResult.builder().createdMailIds(Arrays.asList(1L, 2L)).build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> result.getCreatedMailIds().add(3L));
    }

    @Test
    void getItemErrors_shouldBeUnmodifiable() {
        ItemDeliveryError err =
                ItemDeliveryError.builder().itemTemplateId(57L).count(1L).build();
        SendMailResult result = SendMailResult.builder()
                .itemErrors(Collections.singletonList(err))
                .build();

        assertThrows(
                UnsupportedOperationException.class,
                () -> result.getItemErrors().add(err));
    }

    @Test
    void constructor_shouldDefensivelyCopyMailIds() {
        java.util.List<Long> mutable = new java.util.ArrayList<Long>(Arrays.asList(1L, 2L));
        SendMailResult result = new SendMailResult(mutable, null);

        mutable.add(3L);

        assertEquals(2, result.getCreatedMailIds().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        SendMailResult original =
                SendMailResult.builder().createdMailIds(Arrays.asList(1L, 2L)).build();
        SendMailResult copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnCreatedMailIds() {
        SendMailResult a = SendMailResult.builder()
                .createdMailIds(Collections.singletonList(1L))
                .build();
        SendMailResult b = SendMailResult.builder()
                .createdMailIds(Collections.singletonList(2L))
                .build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        SendMailResult a =
                SendMailResult.builder().createdMailIds(Arrays.asList(1L, 2L)).build();
        SendMailResult b =
                SendMailResult.builder().createdMailIds(Arrays.asList(1L, 2L)).build();

        assertEquals(a.hashCode(), b.hashCode());
    }
}
