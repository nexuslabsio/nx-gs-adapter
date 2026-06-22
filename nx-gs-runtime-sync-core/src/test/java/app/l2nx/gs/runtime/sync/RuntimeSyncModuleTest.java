package app.l2nx.gs.runtime.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.rest.SyncTopics;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.adapter.api.spi.RuntimeEntityMapping;
import app.l2nx.gs.adapter.api.spi.RuntimeRow;
import app.l2nx.gs.adapter.api.spi.RuntimeStateProvider;
import app.l2nx.gs.runtime.sync.engine.publish.KafkaSender;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

class RuntimeSyncModuleTest {

    private static final Map<String, String> CHARACTER_TOPIC =
            Collections.singletonMap("character", "bohpts.gs.sync.runtime.character");

    private static final ConnectContext CTX_WITH_TOPIC =
            ctx(SyncTopics.builder().runtime(CHARACTER_TOPIC).build());
    private static final ConnectContext CTX_NO_TOPIC = ctx(null);

    private static final KafkaSender NEVER_CALLED = (topic, key, value, callback) -> {
        throw new AssertionError("KafkaSender must not be called from constructor-only paths");
    };

    @Test
    void name_shouldBe_runtimeSync() {
        assertEquals("runtime-sync", new RuntimeSyncModule().name());
    }

    @Test
    void onConnect_shouldDisable_whenSyncTopicsAbsent() {
        RuntimeSyncModule module = build(emptyProviders());

        module.onConnect(CTX_NO_TOPIC);

        assertEquals(RuntimeSyncModule.STATE_DISABLED, module.stateForTesting());
    }

    @Test
    void onConnect_shouldDisable_whenRuntimeNamespaceEmpty() {
        RuntimeSyncModule module = build(emptyProviders());

        module.onConnect(ctx(
                SyncTopics.builder().db(Collections.singletonMap("clan", "x")).build()));

        assertEquals(RuntimeSyncModule.STATE_DISABLED, module.stateForTesting());
    }

    @Test
    void onConnect_shouldDisable_whenZeroProviders() {
        RuntimeSyncModule module = build(emptyProviders());

        module.onConnect(CTX_WITH_TOPIC);

        assertEquals(RuntimeSyncModule.STATE_DISABLED, module.stateForTesting());
    }

    @Test
    void onConnect_shouldFail_whenMultipleProviders() {
        RuntimeSyncModule module = build(() -> Arrays.asList(stubProvider(), stubProvider()));

        module.onConnect(CTX_WITH_TOPIC);

        assertEquals(RuntimeSyncModule.STATE_FAILED, module.stateForTesting());
    }

    @Test
    void onConnect_shouldActivate_whenSingleProviderResolves() {
        RuntimeSyncModule module = build(() -> Collections.singletonList(stubProvider()));

        module.onConnect(CTX_WITH_TOPIC);

        assertEquals(RuntimeSyncModule.STATE_ACTIVE, module.stateForTesting());
    }

    @Test
    void start_shouldDisable_whenProviderHasNoMappings() {
        RuntimeStateProvider empty = new RuntimeStateProvider() {
            @Override
            public String schemaName() {
                return "test";
            }

            @Override
            public List<RuntimeEntityMapping<?>> mappings() {
                return Collections.emptyList();
            }
        };
        RuntimeSyncModule module = build(() -> Collections.singletonList(empty));

        module.onConnect(CTX_WITH_TOPIC);
        module.start();

        assertEquals(RuntimeSyncModule.STATE_DISABLED, module.stateForTesting());
    }

    @Test
    void currentStatus_shouldCarryStateAndName() {
        RuntimeSyncModule module = new RuntimeSyncModule();

        ModuleStatus status = module.currentStatus();

        assertEquals("runtime-sync", status.getName());
        assertEquals("INIT", status.getState());
    }

    private static RuntimeSyncModule build(Supplier<List<RuntimeStateProvider>> providers) {
        Function<String, String> noSysprops = k -> null;
        return new RuntimeSyncModule(providers, noSysprops, NEVER_CALLED);
    }

    private static Supplier<List<RuntimeStateProvider>> emptyProviders() {
        return Collections::emptyList;
    }

    private static RuntimeStateProvider stubProvider() {
        return new RuntimeStateProvider() {
            @Override
            public String schemaName() {
                return "test";
            }

            @Override
            public List<RuntimeEntityMapping<?>> mappings() {
                List<RuntimeEntityMapping<?>> list = new ArrayList<RuntimeEntityMapping<?>>();
                list.add(stubMapping());
                return list;
            }
        };
    }

    private static RuntimeEntityMapping<String> stubMapping() {
        return new RuntimeEntityMapping<String>() {
            @Override
            public String entityName() {
                return "character";
            }

            @Override
            public Class<String> dtoType() {
                return String.class;
            }

            @Override
            public Iterable<RuntimeRow<String>> snapshot() {
                return Collections.emptyList();
            }

            @Override
            public long hash(String dto) {
                return 0L;
            }
        };
    }

    private static ConnectContext ctx(SyncTopics syncTopics) {
        return ConnectContext.builder()
                .tenantId(UUID.randomUUID())
                .tenantSlug("acme")
                .serverId(UUID.randomUUID())
                .serverSlug("primary")
                .serverName("Acme Primary")
                .adapterVersion("0.1.0")
                .syncTopics(syncTopics)
                .build();
    }
}
