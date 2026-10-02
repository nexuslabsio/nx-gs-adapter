package app.l2nx.gs.adapter.api.spi.capability;

/** Must not block beyond a queue submission or throw. */
@FunctionalInterface
public interface NxGameDataTrigger {

    void run();
}
