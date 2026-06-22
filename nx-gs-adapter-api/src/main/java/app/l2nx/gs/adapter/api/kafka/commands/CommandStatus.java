package app.l2nx.gs.adapter.api.kafka.commands;

/**
 * Outcome of an inbound {@link NxCommand}. Wire form: enum constant name.
 * Consumers SHOULD treat unknown values as a generic failure for
 * forward-compat. See the commands guide for picking the right status;
 * {@link #UNSUPPORTED_COMMAND} is adapter-emitted only.
 */
public enum CommandStatus {
    OK(Tier.OK),

    NOT_FOUND(Tier.CLIENT_ERROR),
    INVALID_STATE(Tier.CLIENT_ERROR),
    FORBIDDEN(Tier.CLIENT_ERROR),
    VALIDATION_FAILED(Tier.CLIENT_ERROR),
    RATE_LIMITED(Tier.CLIENT_ERROR),
    UNSUPPORTED_COMMAND(Tier.CLIENT_ERROR),

    UNAVAILABLE(Tier.SERVER_ERROR),
    INTERNAL_ERROR(Tier.SERVER_ERROR);

    private final Tier tier;

    CommandStatus(Tier tier) {
        this.tier = tier;
    }

    public Tier tier() {
        return tier;
    }

    public boolean isOk() {
        return tier == Tier.OK;
    }

    /**
     * Coarse HTTP-aligned classification for callers routing on
     * retry / surface-to-user / alert-ops.
     */
    public enum Tier {
        OK,
        CLIENT_ERROR,
        SERVER_ERROR
    }
}
