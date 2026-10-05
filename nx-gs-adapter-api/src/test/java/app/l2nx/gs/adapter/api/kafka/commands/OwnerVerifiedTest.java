package app.l2nx.gs.adapter.api.kafka.commands;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import app.l2nx.gs.adapter.api.kafka.commands.chat.ChatAudiences;
import app.l2nx.gs.adapter.api.kafka.commands.chat.SendChatMessageCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.BuyFromPrivateStoreCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.StartPrivateStorePackageSellCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.StartPrivateStoreSellCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.StopPrivateStoreCommand;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.model.BuyLine;
import app.l2nx.gs.adapter.api.kafka.commands.privatestore.model.SellLine;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class OwnerVerifiedTest {

    private static final Gson GSON = new GsonBuilder()
            .registerTypeAdapter(
                    Instant.class, (JsonSerializer<Instant>) (src, type, ctx) -> new JsonPrimitive(src.toString()))
            .registerTypeAdapter(
                    Instant.class, (JsonDeserializer<Instant>) (json, type, ctx) -> Instant.parse(json.getAsString()))
            .create();

    private static Stream<Arguments> verifiedCommands() {
        SellLine sell = new SellLine(1, 1L, 10L);
        BuyLine buy = BuyLine.builder()
                .itemId(1)
                .itemTemplateId(57L)
                .count(1L)
                .unitPriceAdena(100L)
                .build();
        return Stream.of(
                Arguments.of(new StopPrivateStoreCommand(1, true)),
                Arguments.of(new StartPrivateStoreSellCommand(1, "t", Collections.singletonList(sell), true)),
                Arguments.of(new StartPrivateStorePackageSellCommand(1, "t", Collections.singletonList(sell), true)),
                Arguments.of(new BuyFromPrivateStoreCommand(
                        1,
                        2,
                        Collections.singletonList(buy),
                        5,
                        Instant.parse("2026-08-11T12:00:00Z"),
                        "Courier",
                        "Subject",
                        "Body",
                        true)),
                Arguments.of(new SendChatMessageCommand(
                        UUID.randomUUID(),
                        "CLAN",
                        ChatAudiences.CLAN,
                        7L,
                        100L,
                        null,
                        "MINIAPP",
                        "hello",
                        null,
                        true)));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("verifiedCommands")
    void command_shouldImplementOwnerVerified(OwnerVerified command) {
        assertTrue(command.isOwnerVerified());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("verifiedCommands")
    void gsonRoundTrip_shouldKeepOwnerVerified_whenTrue(Object command) {
        Object copy = GSON.fromJson(GSON.toJson(command), command.getClass());

        assertEquals(command, copy);
        assertTrue(assertInstanceOf(OwnerVerified.class, copy).isOwnerVerified());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("verifiedCommands")
    void gsonDeserialize_shouldYieldNotVerified_whenFieldAbsent(Object command) {
        JsonObject json = GSON.toJsonTree(command).getAsJsonObject();
        json.remove("ownerVerified");

        Object copy = GSON.fromJson(json, command.getClass());

        assertFalse(assertInstanceOf(OwnerVerified.class, copy).isOwnerVerified());
    }

    @Test
    void builder_shouldDefaultToNotVerified() {
        assertFalse(StopPrivateStoreCommand.builder().charId(1).build().isOwnerVerified());
    }
}
