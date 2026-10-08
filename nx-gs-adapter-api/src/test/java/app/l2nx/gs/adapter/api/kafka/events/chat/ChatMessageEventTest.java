package app.l2nx.gs.adapter.api.kafka.events.chat;

import static org.junit.jupiter.api.Assertions.*;

import app.l2nx.gs.adapter.api.kafka.sync.db.item.ItemAugmentationDbDto;
import com.google.gson.Gson;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class ChatMessageEventTest {

    private static final Gson GSON = new Gson();

    private static ChatItemSnapshot snapshot() {
        Map<String, Integer> attributes = new LinkedHashMap<String, Integer>();
        attributes.put("FIRE", 150);
        return ChatItemSnapshot.builder()
                .itemObjectId(268500001L)
                .itemTemplateId(6577)
                .enchantLevel(7)
                .attributes(attributes)
                .augmentation(new ItemAugmentationDbDto(16341, 18000))
                .count(1L)
                .build();
    }

    private static ChatMessageEvent.Builder base() {
        return ChatMessageEvent.builder()
                .eventId(UUID.randomUUID())
                .charId(268437521L)
                .charName("Vasya")
                .channel(WellKnownChatChannels.GENERAL)
                .text("hi");
    }

    @Nested
    class NewFields {

        @Test
        void builder_shouldCarryNewFields() {
            ChatMessageEvent event = base().senderDisplayName("Vasya*")
                    .recipientCharacterIds(Arrays.asList(1L, 2L))
                    .items(Collections.singletonList(snapshot()))
                    .build();

            assertEquals("Vasya*", event.getSenderDisplayName());
            assertEquals(Arrays.asList(1L, 2L), event.getRecipientCharacterIds());
            assertEquals(Collections.singletonList(snapshot()), event.getItems());
        }

        @Test
        void getters_shouldReturnNull_whenOmitted() {
            ChatMessageEvent event = base().build();

            assertNull(event.getSenderDisplayName());
            assertNull(event.getRecipientCharacterIds());
            assertNull(event.getItems());
        }

        @Test
        void lists_shouldBeDefensivelyCopiedAndUnmodifiable() {
            List<Long> recipients = new ArrayList<Long>(Arrays.asList(1L, 2L));
            ChatMessageEvent event = base().recipientCharacterIds(recipients)
                    .items(new ArrayList<ChatItemSnapshot>(Collections.singletonList(snapshot())))
                    .build();

            recipients.add(3L);

            assertEquals(2, event.getRecipientCharacterIds().size());
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> event.getRecipientCharacterIds().add(4L));
            assertThrows(
                    UnsupportedOperationException.class, () -> event.getItems().add(snapshot()));
        }

        @Test
        void toBuilder_shouldRoundtripAllFields() {
            ChatMessageEvent original = base().senderDisplayName("Vasya*")
                    .recipientCharacterIds(Arrays.asList(1L, 2L))
                    .items(Collections.singletonList(snapshot()))
                    .build();

            assertEquals(original, original.toBuilder().build());
        }

        @Test
        void equals_shouldDistinguishSenderDisplayName() {
            UUID id = UUID.randomUUID();
            ChatMessageEvent a = base().eventId(id).senderDisplayName("Vasya").build();
            ChatMessageEvent b = base().eventId(id).senderDisplayName("Vasya*").build();

            assertNotEquals(a, b);
        }
    }

    @Nested
    class GsonRoundtrip {

        @Test
        void roundtrip_shouldPreserveNewFields() {
            ChatMessageEvent original = base().senderDisplayName("Vasya*")
                    .recipientCharacterIds(Arrays.asList(1L, 2L))
                    .items(Collections.singletonList(snapshot()))
                    .build();

            ChatMessageEvent parsed = GSON.fromJson(GSON.toJson(original), ChatMessageEvent.class);

            assertEquals(original, parsed);
        }

        @Test
        void fromJson_shouldReadNewFieldsAsNull_whenOldPayload() {
            String old = "{\"eventId\":\"" + UUID.randomUUID()
                    + "\",\"charId\":5,\"charName\":\"Vasya\",\"channel\":\"CLAN\",\"text\":\"hi\"}";

            ChatMessageEvent parsed = GSON.fromJson(old, ChatMessageEvent.class);

            assertEquals("hi", parsed.getText());
            assertNull(parsed.getSenderDisplayName());
            assertNull(parsed.getRecipientCharacterIds());
            assertNull(parsed.getItems());
        }
    }

    @Nested
    class ItemSnapshot {

        @Test
        void attributes_shouldBeEmpty_whenNullPassed() {
            ChatItemSnapshot item = ChatItemSnapshot.builder().itemObjectId(1L).build();

            assertTrue(item.getAttributes().isEmpty());
            assertNull(item.getAugmentation());
        }

        @Test
        void count_shouldBeNull_whenHostDoesNotReportIt() {
            assertNull(ChatItemSnapshot.builder().itemObjectId(1L).build().getCount());
        }

        @Test
        void equals_shouldDiffer_whenCountDiffers() {
            assertNotEquals(snapshot(), snapshot().toBuilder().count(2L).build());
        }

        @Test
        void attributes_shouldBeUnmodifiableCopy() {
            Map<String, Integer> source = new LinkedHashMap<String, Integer>();
            source.put("WATER", 100);
            ChatItemSnapshot item =
                    ChatItemSnapshot.builder().attributes(source).build();

            source.put("FIRE", 50);

            assertEquals(1, item.getAttributes().size());
            assertThrows(
                    UnsupportedOperationException.class,
                    () -> item.getAttributes().put("DARK", 1));
        }

        @Test
        void augmentation_shouldAllowNullOption2() {
            ItemAugmentationDbDto augmentation = new ItemAugmentationDbDto(10, null);

            assertEquals(10, augmentation.getOption1Id());
            assertNull(augmentation.getOption2Id());
            assertEquals(augmentation, augmentation.toBuilder().build());
        }

        @Test
        void toBuilder_shouldRoundtripAllFields() {
            ChatItemSnapshot original = snapshot();

            assertEquals(original, original.toBuilder().build());
            assertEquals(original.hashCode(), original.toBuilder().build().hashCode());
        }
    }

    @Test
    void allianceIdKey_shouldBeCamelCase() {
        assertEquals("allianceId", ChatMetadataKeys.ALLIANCE_ID);
    }
}
