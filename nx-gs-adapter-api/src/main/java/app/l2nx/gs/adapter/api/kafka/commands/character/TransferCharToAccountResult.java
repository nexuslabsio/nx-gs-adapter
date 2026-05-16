package app.l2nx.gs.adapter.api.kafka.commands.character;

import java.util.Objects;

/**
 * Success payload of {@link TransferCharToAccountCommand}. Echoes the rebound
 * character + the new account name, plus a flag indicating whether a
 * force-logout was needed (the character was online at command time).
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class TransferCharToAccountResult {

    private final Long charId;
    private final String newAccountName;
    private final boolean wasLoggedOut;

    public TransferCharToAccountResult(Long charId,
                                       String newAccountName,
                                       boolean wasLoggedOut) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (newAccountName == null) {
            throw new IllegalArgumentException("newAccountName is required");
        }
        this.charId = charId;
        this.newAccountName = newAccountName;
        this.wasLoggedOut = wasLoggedOut;
    }

    public Long getCharId() {
        return charId;
    }

    public String getNewAccountName() {
        return newAccountName;
    }

    /**
     * {@code true} when the character was online at handler-invocation time
     * and the host issued a force-logout before the rebind. {@code false}
     * when the character was already offline.
     */
    public boolean isWasLoggedOut() {
        return wasLoggedOut;
    }

    public Builder toBuilder() {
        return new Builder()
                .charId(charId)
                .newAccountName(newAccountName)
                .wasLoggedOut(wasLoggedOut);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TransferCharToAccountResult)) return false;
        TransferCharToAccountResult that = (TransferCharToAccountResult) o;
        return wasLoggedOut == that.wasLoggedOut
                && Objects.equals(charId, that.charId)
                && Objects.equals(newAccountName, that.newAccountName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, newAccountName, wasLoggedOut);
    }

    @Override
    public String toString() {
        return "TransferCharToAccountResult[charId=" + charId
                + ", newAccountName=" + newAccountName
                + ", wasLoggedOut=" + wasLoggedOut + "]";
    }

    public static final class Builder {
        private Long charId;
        private String newAccountName;
        private boolean wasLoggedOut;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder newAccountName(String newAccountName) {
            this.newAccountName = newAccountName;
            return this;
        }

        public Builder wasLoggedOut(boolean wasLoggedOut) {
            this.wasLoggedOut = wasLoggedOut;
            return this;
        }

        public TransferCharToAccountResult build() {
            return new TransferCharToAccountResult(charId, newAccountName, wasLoggedOut);
        }
    }
}
