package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.util.Objects;

/**
 * Wire DTO for one row of {@code character_subclasses} (or its tenant
 * equivalent), carried inside {@link CharacterDbDto#getSubclasses()}.
 *
 * <p>Surfaces the identifying-and-versioning pair: {@code classId} (which
 * subclass) and {@code level} (current subclass level). Volatile columns
 * ({@code exp}, {@code sp}) are intentionally not modeled — they tick on
 * every kill and would generate an UPDATE storm per cycle. Other source
 * columns ({@code class_index}) are details that platform consumers do
 * not need in the v1 wire.</p>
 *
 * <p>Subclass rows whose {@code class_id} resolves to a value outside
 * {@link CharacterClass}'s canonical set are dropped by the schema
 * provider before assembly — every row that reaches the wire has a
 * non-null {@code classId}.</p>
 *
 * @deprecated superseded by {@link CharacterClassDbDto}, which covers the
 *     character's whole class roster (main class included) and carries
 *     {@code exp} / {@code sp}. Kept alive for one release so a platform
 *     deployed ahead of the schema providers can still read events emitted
 *     by an older adapter. Removed once every schema provider emits
 *     {@link CharacterDbDto#getClasses()} — for bohpts, the morning
 *     game-server restart that ships the new adapter.
 */
@Deprecated
public final class CharacterSubclassDbDto {

    private final CharacterClass classId;
    private final int level;

    public CharacterSubclassDbDto(CharacterClass classId, int level) {
        this.classId = classId;
        this.level = level;
    }

    /**
     * Subclass class identifier — {@code NOT NULL} on the wire (rows with
     * unknown source class IDs are dropped by the schema provider).
     */
    public CharacterClass getClassId() {
        return classId;
    }

    /**
     * Subclass level — {@code NOT NULL} on the source side.
     */
    public int getLevel() {
        return level;
    }

    public Builder toBuilder() {
        return new Builder().classId(classId).level(level);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterSubclassDbDto)) return false;
        CharacterSubclassDbDto that = (CharacterSubclassDbDto) o;
        return classId == that.classId && level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classId, level);
    }

    @Override
    public String toString() {
        return "CharacterSubclassDbDto[classId=" + classId + ", level=" + level + "]";
    }

    public static final class Builder {
        private CharacterClass classId;
        private int level;

        public Builder classId(CharacterClass classId) {
            this.classId = classId;
            return this;
        }

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public CharacterSubclassDbDto build() {
            return new CharacterSubclassDbDto(classId, level);
        }
    }
}
