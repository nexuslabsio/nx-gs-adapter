package app.l2nx.gs.adapter.api.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import org.junit.jupiter.api.Test;

class AdapterModuleTest {

    @Test
    void currentStatus_default_shouldReportNameActiveAndEmptyStats() {
        AdapterModule module = stubModule("db-sync");

        ModuleStatus status = module.currentStatus();

        assertEquals("db-sync", status.getName());
        assertEquals("ACTIVE", status.getState());
        assertSame(ModuleStatus.Stats.empty(), status.getStats());
    }

    private static AdapterModule stubModule(final String name) {
        return new AdapterModule() {
            @Override
            public String name() {
                return name;
            }

            @Override
            public void onConnect(ConnectContext ctx) {}

            @Override
            public void start() {}

            @Override
            public void stop() {}

            @Override
            public void onDisconnect() {}
        };
    }
}
