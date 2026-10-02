package app.l2nx.gs.adapter.core.modules;

import app.l2nx.gs.adapter.api.kafka.ops.model.ModuleStatus;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.gs.log.NxLog;
import app.l2nx.gs.log.NxLogFactory;
import java.util.*;

/** Module health is tracked apart from self-reported status: a throwing onConnect/start marks it FAILED and pins its currentStatus() to {name, "FAILED", empty}. */
public final class ModuleRegistry {

    private static final NxLog log = NxLogFactory.getLogger(ModuleRegistry.class);

    enum LifecycleState {
        HEALTHY,
        FAILED
    }

    private final Object lock = new Object();
    private final List<AdapterModule> modules = new ArrayList<AdapterModule>();
    private final Map<String, LifecycleState> states = new HashMap<String, LifecycleState>();

    /** Loads on adapter-core's own classloader to avoid host-CL surprises. */
    public void discover() {
        ClassLoader saved = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ModuleRegistry.class.getClassLoader());
            ServiceLoader<AdapterModule> loader = ServiceLoader.load(AdapterModule.class);
            List<AdapterModule> found = new ArrayList<AdapterModule>();
            for (AdapterModule m : loader) {
                found.add(m);
            }
            installFound(found);
        } finally {
            Thread.currentThread().setContextClassLoader(saved);
        }
    }

    void discoverFrom(List<AdapterModule> input) {
        installFound(new ArrayList<AdapterModule>(input));
    }

    private void installFound(List<AdapterModule> found) {
        found.sort(new Comparator<AdapterModule>() {
            @Override
            public int compare(AdapterModule a, AdapterModule b) {
                return a.name().compareTo(b.name());
            }
        });
        synchronized (lock) {
            modules.clear();
            modules.addAll(found);
            states.clear();
            for (AdapterModule m : found) {
                states.put(m.name(), LifecycleState.HEALTHY);
            }
        }
        if (found.isEmpty()) {
            log.info("No AdapterModule on classpath — adapter runs with empty enabledModules");
        } else {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < found.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append(found.get(i).name());
            }
            log.info("Discovered {} AdapterModule(s): [{}]", found.size(), sb.toString());
        }
    }

    /** Two-phase: start() runs only for modules whose onConnect succeeded; an onConnect failure marks the module FAILED. */
    public void connect(ConnectContext ctx) {
        List<AdapterModule> snapshot = snapshot();
        for (AdapterModule m : snapshot) {
            invokeAndTrack(m, "onConnect", new Runnable() {
                @Override
                public void run() {
                    m.onConnect(ctx);
                }
            });
        }
        for (AdapterModule m : snapshot) {
            if (stateOf(m.name()) == LifecycleState.HEALTHY) {
                invokeAndTrack(m, "start", new Runnable() {
                    @Override
                    public void run() {
                        m.start();
                    }
                });
            }
        }
    }

    /** Reverse discovery order; failures are logged and don't abort the sequence. */
    public void shutdown() {
        List<AdapterModule> reversed = new ArrayList<AdapterModule>(snapshot());
        Collections.reverse(reversed);
        for (AdapterModule m : reversed) {
            invokeIgnoringFailure(m, "stop", new Runnable() {
                @Override
                public void run() {
                    m.stop();
                }
            });
        }
        for (AdapterModule m : reversed) {
            invokeIgnoringFailure(m, "onDisconnect", new Runnable() {
                @Override
                public void run() {
                    m.onDisconnect();
                }
            });
        }
    }

    /** Falls back to {name, "FAILED", empty} when the module is marked FAILED, throws, or returns null. */
    public List<ModuleStatus> currentStatuses() {
        List<AdapterModule> snapshot = snapshot();
        List<ModuleStatus> result = new ArrayList<ModuleStatus>(snapshot.size());
        for (AdapterModule m : snapshot) {
            if (stateOf(m.name()) == LifecycleState.FAILED) {
                result.add(failedStatus(m.name()));
                continue;
            }
            ModuleStatus reported;
            try {
                reported = m.currentStatus();
            } catch (Throwable t) {
                log.error(
                        "Module {}.currentStatus threw {}",
                        m.name(),
                        t.getClass().getName());
                result.add(failedStatus(m.name()));
                continue;
            }
            result.add(reported != null ? reported : failedStatus(m.name()));
        }
        return result;
    }

    public List<AdapterModule> modules() {
        synchronized (lock) {
            return Collections.unmodifiableList(new ArrayList<AdapterModule>(modules));
        }
    }

    private List<AdapterModule> snapshot() {
        synchronized (lock) {
            return new ArrayList<AdapterModule>(modules);
        }
    }

    private LifecycleState stateOf(String name) {
        synchronized (lock) {
            LifecycleState s = states.get(name);
            return s != null ? s : LifecycleState.FAILED;
        }
    }

    private void invokeAndTrack(AdapterModule m, String hookName, Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            log.error(
                    "Module {}.{} threw {}: {}",
                    m.name(),
                    hookName,
                    t.getClass().getName(),
                    t.getMessage());
            synchronized (lock) {
                states.put(m.name(), LifecycleState.FAILED);
            }
        }
    }

    private void invokeIgnoringFailure(AdapterModule m, String hookName, Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            log.error(
                    "Module {}.{} threw {} during shutdown: {}",
                    m.name(),
                    hookName,
                    t.getClass().getName(),
                    t.getMessage());
        }
    }

    private static ModuleStatus failedStatus(String name) {
        return ModuleStatus.builder()
                .name(name)
                .state("FAILED")
                .stats(ModuleStatus.Stats.empty())
                .build();
    }
}
