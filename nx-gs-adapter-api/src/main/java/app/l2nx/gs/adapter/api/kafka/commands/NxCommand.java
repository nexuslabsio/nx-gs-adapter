package app.l2nx.gs.adapter.api.kafka.commands;

/**
 * Marker interface for inbound command DTOs travelling on the
 * {@code <tenant>.gs.commands} Kafka topic from the platform's web side to
 * the game-server core.
 *
 * <p>Every concrete command DTO ships in
 * {@code app.l2nx.gs.adapter.api.kafka.commands.<group>.*} (group = code-org
 * bucket: {@code character}, {@code item}, {@code mail}, {@code account}) and
 * implements this marker. The Kafka topic remains single — the package split
 * exists for Javadoc / IDE discovery only.</p>
 *
 * <p>Routing on the adapter side is by {@code Nx-Message-Type} Kafka header
 * (value = simple class name, e.g. {@code "KickCommand"}). Adapter-core
 * looks up the handler via {@code NxCommands.on(Class<C>, CommandHandler<C, R>)}
 * registrations populated by host code in its {@code onConnect(ctx)} callback.</p>
 *
 * <p>Replies use the {@link CommandResult} envelope and travel on the
 * {@code <tenant>.gs.commands.replies} topic, carrying the inbound
 * {@code Nx-Correlation-Id} header so the platform can route the reply back
 * to the originating web-side request.</p>
 *
 * <p>This slice ships only the marker — concrete DTO catalog
 * ({@code KickCommand}, {@code SendMailCommand}, …) lands in a follow-up
 * slice once the legacy {@code bohpts-rabbitmq} command set is redesigned.</p>
 *
 * @see CommandResult
 * @see app.l2nx.gs.adapter.api.spi.CommandHandler
 * @see app.l2nx.gs.adapter.api.spi.NxCommands
 */
public interface NxCommand {
}
