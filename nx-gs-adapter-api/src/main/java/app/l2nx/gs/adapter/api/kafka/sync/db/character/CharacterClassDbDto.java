package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassKind;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one class a character owns, carried in {@link CharacterDbDto#getClasses()}.
 *
 * Roster shape is normalized by the schema provider (builds differ on where the main class is
 * stored). The class currently played is {@link CharacterDbDto#getClassId()}, not a flag here.
 */
public final class CharacterClassDbDto {

    private final CharacterClass classId;
    private final CharacterClassKind kind;
    private final @Nullable Integer level;
    private final @Nullable Long exp;
    private final @Nullable Long sp;

    public CharacterClassDbDto(
            CharacterClass classId,
            CharacterClassKind kind,
            @Nullable Integer level,
            @Nullable Long exp,
            @Nullable Long sp) {
        this.classId = Objects.requireNonNull(classId, "CharacterClassDbDto.classId is required");
        this.kind = Objects.requireNonNull(kind, "CharacterClassDbDto.kind is required");
        this.level = level;
        this.exp = exp;
        this.sp = sp;
    }

    /** Non-null on the wire: entries with unknown source class ids are dropped by the provider. */
    public CharacterClass getClassId() {
        return classId;
    }

    public CharacterClassKind getKind() {
        return kind;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    /** Unhashed ride-along: never triggers a sync event, value is as of the last full store at the source. */
    public @Nullable Long getExp() {
        return exp;
    }

    /** Unhashed ride-along, same as {@link #getExp()}. */
    public @Nullable Long getSp() {
        return sp;
    }

    public Builder toBuilder() {
        return new Builder().classId(classId).kind(kind).level(level).exp(exp).sp(sp);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterClassDbDto)) return false;
        CharacterClassDbDto that = (CharacterClassDbDto) o;
        return classId == that.classId
                && kind == that.kind
                && Objects.equals(level, that.level)
                && Objects.equals(exp, that.exp)
                && Objects.equals(sp, that.sp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classId, kind, level, exp, sp);
    }

    @Override
    public String toString() {
        return "CharacterClassDbDto[classId=" + classId
                + ", kind=" + kind
                + ", level=" + level
                + ", exp=" + exp
                + ", sp=" + sp + "]";
    }

    public static final class Builder {
        private @Nullable CharacterClass classId;
        private @Nullable CharacterClassKind kind;
        private @Nullable Integer level;
        private @Nullable Long exp;
        private @Nullable Long sp;

        public Builder classId(CharacterClass classId) {
            this.classId = classId;
            return this;
        }

        public Builder kind(CharacterClassKind kind) {
            this.kind = kind;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        public Builder exp(@Nullable Long exp) {
            this.exp = exp;
            return this;
        }

        public Builder sp(@Nullable Long sp) {
            this.sp = sp;
            return this;
        }

        public CharacterClassDbDto build() {
            return new CharacterClassDbDto(classId, kind, level, exp, sp);
        }
    }
}
