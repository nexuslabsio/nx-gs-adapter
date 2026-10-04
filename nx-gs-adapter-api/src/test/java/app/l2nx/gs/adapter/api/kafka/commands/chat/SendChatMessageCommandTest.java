package app.l2nx.gs.adapter.api.kafka.commands.chat;

import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class SendChatMessageCommandTest {

    private static SendChatMessageCommand.Builder base() {
        return SendChatMessageCommand.builder()
                .messageId(UUID.randomUUID())
                .channel("CLAN")
                .audience(ChatAudiences.CLAN)
                .audienceId(7L)
                .senderCharacterId(100L)
                .source("MINIAPP")
                .text("hello");
    }

    @Nested
    class Sender {

        @Test
        void build_shouldAccept_whenCharacterSetAndDisplayNameNull() {
            SendChatMessageCommand command = base().build();

            assertNull(command.getSenderDisplayName());
            assertEquals(100L, command.getSenderCharacterId());
        }

        @Test
        void build_shouldAccept_whenCharacterAndDisplayNameBothSet() {
            SendChatMessageCommand command = base().senderDisplayName("Vasya").build();

            assertEquals("Vasya", command.getSenderDisplayName());
        }

        @Test
        void build_shouldAcceptEmptyDisplayName_whenNoCharacter() {
            SendChatMessageCommand command = base().audience(ChatAudiences.ALL_ONLINE)
                    .audienceId(null)
                    .senderCharacterId(null)
                    .senderDisplayName("")
                    .build();

            assertEquals("", command.getSenderDisplayName());
        }

        @Test
        void build_shouldReject_whenNoCharacterAndDisplayNameNull() {
            assertThrows(
                    NullPointerException.class,
                    () -> base().audience(ChatAudiences.ALL_ONLINE)
                            .audienceId(null)
                            .senderCharacterId(null)
                            .build());
        }
    }

    @Nested
    class Audience {

        @Test
        void build_shouldRequireAudienceId_whenAlliance() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.ALLIANCE)
                            .audienceId(null)
                            .build());
            assertEquals(
                    ChatAudiences.ALLIANCE,
                    base().audience(ChatAudiences.ALLIANCE).build().getAudience());
        }

        @Test
        void build_shouldAcceptParty_whenNoAudienceIdAndSenderSet() {
            SendChatMessageCommand command =
                    base().audience(ChatAudiences.PARTY).audienceId(null).build();

            assertNull(command.getAudienceId());
        }

        @Test
        void build_shouldRejectParty_whenAudienceIdPresent() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.PARTY).build());
        }

        @Test
        void build_shouldRejectParty_whenNoSender() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.PARTY)
                            .audienceId(null)
                            .senderCharacterId(null)
                            .senderDisplayName("")
                            .build());
        }

        @Test
        void build_shouldAcceptCharacter_whenOnlyAudienceId() {
            SendChatMessageCommand command =
                    base().audience(ChatAudiences.CHARACTER).audienceId(55L).build();

            assertEquals(55L, command.getAudienceId());
            assertNull(command.getTargetCharacterName());
        }

        @ParameterizedTest
        @ValueSource(strings = {"Vasya", "Vasya*"})
        void build_shouldAcceptCharacter_whenOnlyTargetName(String name) {
            SendChatMessageCommand command = base().audience(ChatAudiences.CHARACTER)
                    .audienceId(null)
                    .targetCharacterName(name)
                    .build();

            assertEquals(name, command.getTargetCharacterName());
            assertNull(command.getAudienceId());
        }

        @Test
        void build_shouldRejectCharacter_whenBothAudienceIdAndName() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.CHARACTER)
                            .audienceId(55L)
                            .targetCharacterName("Vasya")
                            .build());
        }

        @Test
        void build_shouldRejectCharacter_whenNeither() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.CHARACTER)
                            .audienceId(null)
                            .build());
        }

        @ParameterizedTest
        @ValueSource(strings = {"", "  "})
        void build_shouldRejectCharacter_whenNameBlank(String name) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.CHARACTER)
                            .audienceId(null)
                            .targetCharacterName(name)
                            .build());
        }

        @Test
        void build_shouldRejectTargetName_whenAudienceIsNotCharacter() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().targetCharacterName("Vasya").build());
        }

        @Test
        void build_shouldRejectAudienceId_whenAllOnline() {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> base().audience(ChatAudiences.ALL_ONLINE).build());
        }
    }

    @Nested
    class Compatibility {

        @Test
        void toBuilder_shouldRoundtripAllFields() {
            SendChatMessageCommand original = base().audience(ChatAudiences.CHARACTER)
                    .audienceId(null)
                    .targetCharacterName("Vasya*")
                    .build();

            assertEquals(original, original.toBuilder().build());
        }
    }
}
