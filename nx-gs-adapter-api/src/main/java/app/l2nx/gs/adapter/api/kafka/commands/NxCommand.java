package app.l2nx.gs.adapter.api.kafka.commands;

/**
 * Marker interface for inbound command DTOs travelling on the
 * {@code <tenant>.gs.commands} Kafka topic from the platform's web side to
 * the game-server core.
 *
 * <p>The type parameter {@code R} declares the success-payload type carried
 * inside the {@link CommandResult#getPayload()} slot of the reply envelope
 * — fixed at the command class declaration, not at handler-registration
 * time. This makes the wire reply contract <b>statically typed</b>: the
 * platform-web side and the host-side handler both look at the same
 * {@code NxCommand<R>} type binding and cannot disagree about reply shape.</p>
 *
 * <p>Use {@code NxCommand<Void>} for commands that have no success-payload
 * (success/error envelope only). Use {@code NxCommand<MyPayload>} when the
 * reply carries typed data ({@code CharInfoCommand} → {@code CharInfoPayload},
 * etc.).</p>
 *
 * <p>Every concrete command DTO ships in
 * {@code app.l2nx.gs.adapter.api.kafka.commands.<group>.*} (group = code-org
 * bucket: {@code character}, {@code item}, {@code mail}, {@code account}) and
 * implements this interface. The Kafka topic remains single — the package
 * split exists for Javadoc / IDE discovery only.</p>
 *
 * <p>Routing on the adapter side is by {@code Nx-Message-Type} Kafka header
 * (value = simple class name, e.g. {@code "DeleteItemCommand"}). Adapter-core
 * looks up the handler via
 * {@code NxCommands.on(Class<C>, CommandHandler<C, R>)} registrations
 * populated by host code in its {@code onConnect(ctx)} callback. The
 * registration generic system ensures the handler's reply type matches the
 * command's declared {@code R} at compile time.</p>
 *
 * <p>Replies use the {@link CommandResult} envelope and travel on the
 * {@code <tenant>.gs.commands.replies} topic, carrying the inbound
 * {@code Nx-Correlation-Id} header so the platform can route the reply back
 * to the originating web-side request.</p>
 *
 * @param <R> success-payload type declared by this command; carried inside
 *            {@link CommandResult#getPayload()}. Use {@link Void} for
 *            commands with no typed payload (success or error only).
 * @see CommandResult
 * @see app.l2nx.gs.adapter.api.spi.CommandHandler
 * @see app.l2nx.gs.adapter.api.spi.NxCommands
 */
public interface NxCommand<R> {}
