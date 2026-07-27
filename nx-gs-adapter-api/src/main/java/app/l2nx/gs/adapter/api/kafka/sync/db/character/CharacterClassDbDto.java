package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassKind;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one class a character owns, carried inside
 * {@link CharacterDbDto#getClasses()}. One entry per class: exactly one
 * {@link CharacterClassKind#MAIN}, plus one {@link CharacterClassKind#SUB}
 * per subclass.
 *
 * <p>The roster is assembled by the schema provider, not by consumers.
 * Builds disagree on where the main class lives — most keep it on the
 * character row with only subclasses in a side table, some also store the
 * main class in that side table under class index {@code 0} — and
 * normalizing that is the adapter's job, so every consumer sees the same
 * roster shape.</p>
 *
 * <p>Which class the character is currently playing is NOT modeled here:
 * it is already given by {@link CharacterDbDto#getClassId()}, and a second
 * representation could disagree with the first.</p>
 *
 * <p>Entries whose source class ID resolves to a value outside
 * {@link CharacterClass}'s canonical set are dropped by the schema
 * provider before assembly — every entry that reaches the wire has a
 * non-null {@code classId}.</p>
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

    /**
     * Class identifier — {@code NOT NULL} on the wire (entries with unknown
     * source class IDs are dropped by the schema provider).
     */
    public CharacterClass getClassId() {
        return classId;
    }

    /**
     * Whether this is the character's main class or one of its subclasses —
     * {@code NOT NULL} on the wire.
     */
    public CharacterClassKind getKind() {
        return kind;
    }

    /**
     * Level of this class. {@code null} when the tenant does not surface the
     * source column.
     */
    public @Nullable Integer getLevel() {
        return level;
    }

    /**
     * Experience of this class.
     *
     * <p>Unhashed ride-along: the source column is read during row mapping but
     * is deliberately NOT part of {@code hashedColumns()}, because it advances
     * on every kill and hashing it would storm UPDATEs for every online
     * character each cycle. So it is never what triggers a sync event — the
     * value observed is the one persisted at the source's last full store
     * (logout + periodic autosave), never a per-tick figure.</p>
     *
     * <p>{@code null} when the tenant does not surface the column.</p>
     */
    public @Nullable Long getExp() {
        return exp;
    }

    /**
     * SP of this class. Same unhashed ride-along semantics as
     * {@link #getExp()}.
     */
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
