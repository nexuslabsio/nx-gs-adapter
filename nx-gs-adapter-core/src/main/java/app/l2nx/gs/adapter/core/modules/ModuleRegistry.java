package app.l2nx.gs.adapter.core.modules;

import app.l2nx.gs.adapter.api.kafka.ops.ModuleStatus;
import app.l2nx.gs.adapter.api.spi.AdapterModule;
import app.l2nx.gs.adapter.api.spi.ConnectContext;
import app.l2nx.log.NxLog;
import app.l2nx.log.NxLogFactory;

import java.util.*;

/**
 * Holds the discovered Tier-1 modules and orchestrates their lifecycle. One-shot
 * discovery at adapter bootstrap; cached for the JVM lifetime.
 *
 * <p>Per-module lifecycle health is tracked separately from the module's own
 * self-reported status: when a connect-time hook ({@code onConnect} / {@code start})
 * throws, the registry marks the module {@code FAILED} and short-circuits its future
 * {@code currentStatus()} reports to {@code {name, "FAILED", empty Stats}}. Modules
 * that transition themselves into {@code DEGRADED} (e.g. db-sync's smoke-check
 * failure path) report it via their own {@code currentStatus()} override.</p>
 */
public final class ModuleRegistry {

    private static final NxLog log = NxLogFactory.getLogger(ModuleRegistry.class);

    enum LifecycleState {HEALTHY, FAILED}

    private final Object lock = new Object();
    private final List<AdapterModule> modules = new ArrayList<AdapterModule>();
    private final Map<String, LifecycleState> states = new HashMap<String, LifecycleState>();

    /**
     * Run {@link ServiceLoader#load(Class)} for {@link AdapterModule} on
     * adapter-core's own classloader (avoiding host-CL surprises on JVMs with
     * non-trivial classloader hierarchies) and cache the result, sorted by
     * {@link AdapterModule#name()}.
     */
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

    /**
     * Test seam — installs a pre-built module list, skipping ServiceLoader. Sorts
     * and caches identically to {@link #discover()}.
     */
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

    /**
     * Two-phase connect: invoke {@code onConnect(ctx)} on every module, then iterate
     * again invoking {@code start()} on every module that survived its
     * {@code onConnect}. {@code onConnect} failure transitions the module to
     * {@code FAILED} and skips its {@code start()}.
     */
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

    /**
     * Reverse-discovery-order shutdown: invoke {@code stop()} on every module,
     * then iterate again invoking {@code onDisconnect()}. Failures are logged
     * but do not abort the shutdown sequence.
     */
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

    /**
     * Snapshot of {@link ModuleStatus} per discovered module. For modules the
     * registry has marked {@code FAILED}, returns {@code {name, "FAILED", empty}}
     * without touching the module. Otherwise calls {@code module.currentStatus()}
     * and falls back to {@code {name, "FAILED", empty}} if it throws or returns
     * {@code null}.
     */
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
                log.error("Module {}.currentStatus threw {}", m.name(), t.getClass().getName());
                result.add(failedStatus(m.name()));
                continue;
            }
            result.add(reported != null ? reported : failedStatus(m.name()));
        }
        return result;
    }

    /**
     * Test seam / debugging accessor — returns the discovered modules in name-sorted
     * order. Read-only view; mutation is unsupported.
     */
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
            log.error("Module {}.{} threw {}: {}", m.name(), hookName,
                    t.getClass().getName(), t.getMessage());
            synchronized (lock) {
                states.put(m.name(), LifecycleState.FAILED);
            }
        }
    }

    private void invokeIgnoringFailure(AdapterModule m, String hookName, Runnable r) {
        try {
            r.run();
        } catch (Throwable t) {
            log.error("Module {}.{} threw {} during shutdown: {}", m.name(), hookName,
                    t.getClass().getName(), t.getMessage());
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
