package app.l2nx.gs.adapter.api.kafka.commands.character;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Inbound command instructing the game-server to set, replace, or clear a
 * single character lock (IP / HWID / item-trade). Mirrors the in-game voiced
 * {@code Security} command, which sets a lock via {@code player.setVar(lockVar,
 * value)} and clears it via {@code player.setVar(lockVar, 0)}.
 *
 * <p><b>One lock per call.</b> Exactly one {@code lockType} is named, so a
 * single command can never accidentally touch any lock other than the one it
 * targets. To change several locks the operator issues several commands.</p>
 *
 * <p>Reply:
 * {@link app.l2nx.gs.adapter.api.kafka.commands.CommandResult}{@code <}{@link UpsertCharacterLockResult}{@code >}
 * — carries the post-upsert state of the affected lock. Common error replies:</p>
 * <ul>
 *     <li>{@code NOT_FOUND} — character does not exist.</li>
 *     <li>{@code VALIDATION_FAILED} — wire payload missing {@code charId} or
 *     {@code lockType}, or {@code lockType} is not a recognized lock kind
 *     (Gson defaults a missing wire field to {@code null}; the handler MUST
 *     null-check before applying).</li>
 *     <li>{@code FORBIDDEN} — operation rejected on host policy grounds.</li>
 * </ul>
 *
 * <p><b>Lock type.</b> {@link #getLockType() lockType} is one of
 * {@link app.l2nx.gs.adapter.api.kafka.sync.db.character.WellKnownCharacterLockTypes}
 * ({@code IP} / {@code HWID} / {@code ITEM}) — an {@code UPPER_SNAKE} open
 * string. REQUIRED.</p>
 *
 * <p><b>Value semantics.</b> {@link #getValue() value} is nullable and drives
 * set-vs-clear: a non-blank value sets / replaces the lock to that value; a
 * {@code null} or blank value clears the lock (the host writes the {@code "0"}
 * sentinel, matching the core convention). REQUIRED fields are {@code charId}
 * and {@code lockType} only; {@code value} is optional.</p>
 *
 * <p><b>Partitioning.</b> Routed by {@link #getCharId() charId} on the commands
 * topic — sequential with other character-scoped operations on the same
 * character.</p>
 *
 * <p><b>Idempotency.</b> Set/clear is naturally idempotent (it writes an
 * absolute value, not a delta) — Kafka redelivery on crash recovery re-applies
 * the same final state.</p>
 *
 * <p>Java 8 POJO; final fields; hand-written builder; Gson-friendly via
 * {@code -parameters}-preserved constructor parameter names.</p>
 */
public final class UpsertCharacterLockCommand implements NxCommand<UpsertCharacterLockResult> {

    private final Long charId;
    private final String lockType;
    private final @Nullable String value;

    public UpsertCharacterLockCommand(Long charId, String lockType, @Nullable String value) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (lockType == null) {
            throw new IllegalArgumentException("lockType is required");
        }
        this.charId = charId;
        this.lockType = lockType;
        this.value = value;
    }

    /**
     * Target character's primary key. REQUIRED. Handler MUST emit
     * {@code VALIDATION_FAILED} when the wire payload omits this field
     * (boxed {@code Long} surfaces missing wire data as {@code null}).
     */
    public Long getCharId() {
        return charId;
    }

    /**
     * Lock kind to upsert — a
     * {@link app.l2nx.gs.adapter.api.kafka.sync.db.character.WellKnownCharacterLockTypes}
     * value ({@code IP} / {@code HWID} / {@code ITEM}). REQUIRED. Handler MUST
     * emit {@code VALIDATION_FAILED} when missing or unrecognized.
     */
    public String getLockType() {
        return lockType;
    }

    /**
     * New lock value, or {@code null}/blank to clear. A non-blank value
     * sets / replaces the lock; {@code null} or blank clears it (the host
     * writes the {@code "0"} sentinel). OPTIONAL.
     */
    public @Nullable String getValue() {
        return value;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).lockType(lockType).value(value);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UpsertCharacterLockCommand)) return false;
        UpsertCharacterLockCommand that = (UpsertCharacterLockCommand) o;
        return Objects.equals(charId, that.charId)
                && Objects.equals(lockType, that.lockType)
                && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, lockType, value);
    }

    @Override
    public String toString() {
        return "UpsertCharacterLockCommand[charId=" + charId + ", lockType=" + lockType + ", value=" + value + "]";
    }

    public static final class Builder {
        private Long charId;
        private String lockType;
        private @Nullable String value;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder lockType(String lockType) {
            this.lockType = lockType;
            return this;
        }

        public Builder value(@Nullable String value) {
            this.value = value;
            return this;
        }

        public UpsertCharacterLockCommand build() {
            return new UpsertCharacterLockCommand(charId, lockType, value);
        }
    }
}
