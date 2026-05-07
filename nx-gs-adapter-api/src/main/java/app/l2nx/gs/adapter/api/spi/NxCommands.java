package app.l2nx.gs.adapter.api.spi;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;

/**
 * Adapter-side registration SPI for inbound command handlers. Acquired via
 * {@link ConnectContext#commands()}; the implementation is built into
 * {@code nx-gs-adapter-core} and is NOT a {@code ServiceLoader}-discovered
 * SPI — host code consumes this interface, it does not implement it.
 *
 * <p>Registration window opens when {@code AdapterModule.onConnect(ctx)}
 * fires (or the equivalent host-supplied connect hook). Handlers may also
 * be registered after the consumer thread has started running; the
 * registration table is a thread-safe {@code Map.put} (last write wins on
 * duplicate {@code Class} keys).</p>
 *
 * <p>Routing matches the command's {@code Nx-Message-Type} header (UTF-8
 * simple class name) against the registered {@code Class.getSimpleName()}.
 * Two distinct classes with the same simple name in the catalog will collide
 * — keep simple names unique.</p>
 *
 * <p><b>Game-loop safety contract.</b> {@link #on(Class, CommandHandler)}
 * MUST NOT block the caller longer than a {@code Map.put}, MUST NOT throw
 * unexpected exceptions, and MUST NOT propagate any internal failure up
 * the call chain.</p>
 *
 * <p><b>Disabled commands surface.</b> When {@code MessagingTopics.commandsTopic}
 * is unconfigured, the {@code NxCommands} facade still accepts {@code on(...)}
 * registrations (so host code can call it unconditionally), but no consumer
 * thread runs and registered handlers are never invoked. Operators see
 * the disabled state on the heartbeat {@code commands} module slot.</p>
 *
 * <p>Example:</p>
 * <pre>
 *   ctx.commands().on(KickCommand.class, (cmd, hctx) -&gt; {
 *       hctx.host().sync(() -&gt; {
 *           Player p = GameObjectsStorage.getPlayer(cmd.getCharId().intValue());
 *           if (p != null) p.kick();
 *       });
 *       return CommandResult.success();
 *   });
 * </pre>
 *
 * @see CommandHandler
 * @see CommandContext
 */
public interface NxCommands {

    /**
     * Register {@code handler} for inbound commands whose
     * {@code Nx-Message-Type} header matches {@code type.getSimpleName()}.
     *
     * <p>The bound {@code C extends NxCommand<R>} forces the handler's reply
     * payload type {@code R} to match the command class's declared type at
     * compile time. Handler attempting to return a different payload shape
     * is rejected by the compiler — there is no runtime way to disagree
     * about the reply contract.</p>
     *
     * @param type    concrete command class; {@code type.getSimpleName()} is
     *                the routing key
     * @param handler the dispatcher to invoke; non-null
     * @param <R>     reply payload type, fixed by the command's
     *                {@code NxCommand<R>} declaration
     * @param <C>     command type, must extend {@code NxCommand<R>}
     */
    <R, C extends NxCommand<R>> void on(Class<C> type, CommandHandler<C, R> handler);
}
