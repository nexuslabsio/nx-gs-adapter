package app.l2nx.gs.adapter.api.spi;

/**
 * Module-side SAM registered with {@link NxGameData#registerSnapshotTrigger}.
 * Invoked when the host requests a fresh game-data snapshot via
 * {@link NxGameData#publishSnapshot()}. Implementations MUST NOT block beyond a
 * queue / executor submission and MUST NOT throw — the façade invokes them
 * defensively.
 */
@FunctionalInterface
public interface NxGameDataTrigger {

    void run();
}
