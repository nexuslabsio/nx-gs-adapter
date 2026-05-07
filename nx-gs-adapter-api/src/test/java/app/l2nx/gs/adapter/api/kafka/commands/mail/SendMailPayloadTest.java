package app.l2nx.gs.adapter.api.kafka.commands.mail;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

class SendMailPayloadTest {

    @Test
    void builder_shouldRoundtripFields() {
        ItemDeliveryError err = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).reason("x").build();

        SendMailPayload payload = SendMailPayload.builder()
                .createdMailIds(Arrays.asList(101L, 102L))
                .itemErrors(Collections.singletonList(err))
                .build();

        assertEquals(Arrays.asList(101L, 102L), payload.getCreatedMailIds());
        assertEquals(Collections.singletonList(err), payload.getItemErrors());
    }

    @Test
    void getCreatedMailIds_shouldReturnEmpty_whenNull() {
        SendMailPayload payload = new SendMailPayload(null, null);

        assertTrue(payload.getCreatedMailIds().isEmpty());
    }

    @Test
    void getItemErrors_shouldReturnEmpty_whenNull() {
        SendMailPayload payload = new SendMailPayload(null, null);

        assertTrue(payload.getItemErrors().isEmpty());
    }

    @Test
    void getCreatedMailIds_shouldBeUnmodifiable() {
        SendMailPayload payload = SendMailPayload.builder()
                .createdMailIds(Arrays.asList(1L, 2L))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> payload.getCreatedMailIds().add(3L));
    }

    @Test
    void getItemErrors_shouldBeUnmodifiable() {
        ItemDeliveryError err = ItemDeliveryError.builder()
                .itemTemplateId(57L).count(1L).build();
        SendMailPayload payload = SendMailPayload.builder()
                .itemErrors(Collections.singletonList(err))
                .build();

        assertThrows(UnsupportedOperationException.class,
                () -> payload.getItemErrors().add(err));
    }

    @Test
    void constructor_shouldDefensivelyCopyMailIds() {
        java.util.List<Long> mutable = new java.util.ArrayList<Long>(Arrays.asList(1L, 2L));
        SendMailPayload payload = new SendMailPayload(mutable, null);

        mutable.add(3L);

        assertEquals(2, payload.getCreatedMailIds().size());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        SendMailPayload original = SendMailPayload.builder()
                .createdMailIds(Arrays.asList(1L, 2L))
                .build();
        SendMailPayload copy = original.toBuilder().build();

        assertEquals(original, copy);
    }

    @Test
    void equals_shouldDistinguishOnCreatedMailIds() {
        SendMailPayload a = SendMailPayload.builder()
                .createdMailIds(Collections.singletonList(1L)).build();
        SendMailPayload b = SendMailPayload.builder()
                .createdMailIds(Collections.singletonList(2L)).build();

        assertNotEquals(a, b);
    }

    @Test
    void hashCode_shouldMatchEquals() {
        SendMailPayload a = SendMailPayload.builder()
                .createdMailIds(Arrays.asList(1L, 2L)).build();
        SendMailPayload b = SendMailPayload.builder()
                .createdMailIds(Arrays.asList(1L, 2L)).build();

        assertEquals(a.hashCode(), b.hashCode());
    }
}
