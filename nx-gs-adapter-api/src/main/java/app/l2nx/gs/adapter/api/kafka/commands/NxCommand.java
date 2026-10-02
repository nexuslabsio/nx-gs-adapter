package app.l2nx.gs.adapter.api.kafka.commands;

/**
 * Marker for inbound command DTOs sent on {@code <tenant>.gs.commands}; routed by the {@code Nx-Message-Type}
 * header (simple class name). The package split under {@code commands} is for discovery only; the topic is single.
 *
 * <p>{@code R} is the success-payload type of the {@link CommandResult} reply, fixed at the command declaration
 * so both sides agree on the reply shape.</p>
 *
 * @param <R> success-payload type; {@link Void} when there is none
 * @see CommandResult
 * @see app.l2nx.gs.adapter.api.spi.capability.CommandHandler
 * @see app.l2nx.gs.adapter.api.spi.capability.NxCommands
 */
public interface NxCommand<R> {}
