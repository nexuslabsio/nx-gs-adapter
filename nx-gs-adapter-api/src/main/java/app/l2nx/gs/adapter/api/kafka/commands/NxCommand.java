package app.l2nx.gs.adapter.api.kafka.commands;

/**
 * <em>Architectural placeholder — Phase 2.</em> Marker interface for inbound
 * command DTOs travelling on {@code <tenant>.gs.commands.<domain>} per-domain
 * Kafka topics from the platform's web side to the game-server core.
 *
 * <p>Phase 1 ships only the wire shape: per-domain topics keyed by business
 * domain ({@code char}, {@code clan}, {@code mail}, {@code account}, …),
 * {@code Nx-Message-Type} + {@code Nx-Correlation-Id} headers, and a
 * {@code CommandResultEvent} reply published back to the platform via the
 * events stream (likely {@code <tenant>.gs.events.commands.replies} family,
 * finalized in Phase 2).</p>
 *
 * <p>Result envelope replaces legacy free-form {@code message} field with
 * a structured {@code errorCode} enum
 * ({@code NOT_FOUND} / {@code INVALID_STATE} / {@code FORBIDDEN} /
 * {@code RATE_LIMITED} / {@code UNAVAILABLE} / {@code VALIDATION_FAILED} /
 * {@code INTERNAL_ERROR}), an optional structured
 * {@code errorDetails: Map<String,String>}, and an optional typed
 * {@code payload}.</p>
 *
 * <p>Phase-2 SPI hook (sketch):
 * {@code ctx.commands().on(domain, CommandClass.class, handler)}; handler
 * signature {@code CommandResult<R> handle(C cmd, CommandContext ctx)}.
 * {@code CommandContext} exposes a host-supplied {@code Executor} for
 * game-thread hop when the handler must mutate live game state.</p>
 *
 * <p>No concrete subtype ships in Phase 1.</p>
 */
public interface NxCommand {
}
