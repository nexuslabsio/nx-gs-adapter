package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;

/**
 * Success payload of {@link KickCharacterCommand}.
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class KickCharacterResult {

    private final Long charId;
    private final boolean offlineTrader;

    public KickCharacterResult(Long charId, boolean offlineTrader) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        this.charId = charId;
        this.offlineTrader = offlineTrader;
    }

    public Long getCharId() {
        return charId;
    }

    /**
     * {@code true} when the target was an offline trader: store ended,
     * {@code closeClient} had no client to act on.
     */
    public boolean isOfflineTrader() {
        return offlineTrader;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).offlineTrader(offlineTrader);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof KickCharacterResult)) return false;
        KickCharacterResult that = (KickCharacterResult) o;
        return offlineTrader == that.offlineTrader && Objects.equals(charId, that.charId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, offlineTrader);
    }

    @Override
    public String toString() {
        return "KickCharacterResult[charId=" + charId + ", offlineTrader=" + offlineTrader + "]";
    }

    public static final class Builder {
        private Long charId;
        private boolean offlineTrader;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder offlineTrader(boolean offlineTrader) {
            this.offlineTrader = offlineTrader;
            return this;
        }

        public KickCharacterResult build() {
            return new KickCharacterResult(charId, offlineTrader);
        }
    }
}
