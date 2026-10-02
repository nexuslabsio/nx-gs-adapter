package app.l2nx.gs.adapter.api.kafka.commands.character;

import app.l2nx.gs.adapter.api.kafka.commands.NxCommand;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Sets a character's access level to an absolute value, mirroring the in-game {@code //changelvl}; re-delivery
 * converges on the same state.
 *
 * <p>Reply: {@code CommandResult<SetCharacterAccessLevelResult>}. Errors: {@code NOT_FOUND}, {@code INVALID_STATE}
 * (a login raced the offline write), {@code VALIDATION_FAILED} ({@code charId} / {@code accessLevel} missing,
 * non-integer, negative - bans go through {@code BanCommand} - or unregistered), {@code FORBIDDEN} (above the
 * host's platform-grantable ceiling), {@code UNAVAILABLE} (DB error on the offline path), {@code INTERNAL_ERROR}.</p>
 *
 * <p>Routed by {@code charId}.</p>
 */
public final class SetCharacterAccessLevelCommand implements NxCommand<SetCharacterAccessLevelResult> {

    private final Long charId;
    private final String accessLevel;
    private final @Nullable String staffNotes;

    public SetCharacterAccessLevelCommand(Long charId, String accessLevel, @Nullable String staffNotes) {
        if (charId == null) {
            throw new IllegalArgumentException("charId is required");
        }
        if (accessLevel == null) {
            throw new IllegalArgumentException("accessLevel is required");
        }
        this.charId = charId;
        this.accessLevel = accessLevel;
        this.staffNotes = staffNotes;
    }

    public Long getCharId() {
        return charId;
    }

    /**
     * Opaque, same vocabulary as {@code CharacterDbDto.accessLevel}: numeric text on int builds ({@code "7"}), a role
     * name on string-role builds. The host converts, the platform never interprets.
     */
    public String getAccessLevel() {
        return accessLevel;
    }

    /** Staff-only note: never shown in-game, logged by the host and surfaced on the platform command audit. */
    public @Nullable String getStaffNotes() {
        return staffNotes;
    }

    public Builder toBuilder() {
        return new Builder().charId(charId).accessLevel(accessLevel).staffNotes(staffNotes);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SetCharacterAccessLevelCommand)) return false;
        SetCharacterAccessLevelCommand that = (SetCharacterAccessLevelCommand) o;
        return Objects.equals(charId, that.charId)
                && Objects.equals(accessLevel, that.accessLevel)
                && Objects.equals(staffNotes, that.staffNotes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(charId, accessLevel, staffNotes);
    }

    @Override
    public String toString() {
        return "SetCharacterAccessLevelCommand[charId=" + charId + ", accessLevel=" + accessLevel + ", staffNotes="
                + staffNotes + "]";
    }

    public static final class Builder {
        private Long charId;
        private String accessLevel;
        private @Nullable String staffNotes;

        public Builder charId(Long charId) {
            this.charId = charId;
            return this;
        }

        public Builder accessLevel(String accessLevel) {
            this.accessLevel = accessLevel;
            return this;
        }

        public Builder staffNotes(@Nullable String staffNotes) {
            this.staffNotes = staffNotes;
            return this;
        }

        public SetCharacterAccessLevelCommand build() {
            return new SetCharacterAccessLevelCommand(charId, accessLevel, staffNotes);
        }
    }
}
