package app.l2nx.gs.adapter.api.kafka.sync.gd.classtemplate;

import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassTier;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassType;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Build-agnostic playable-class wire DTO — the common L2 denominator for the static class catalog,
 * carried as the payload of {@code GameDataSyncEvent} on the {@code gd} (game-data) sync stream's
 * {@code classtemplate} entity topic. Each host build supplies its own provider that maps its
 * core's internal class registry into this shape; nothing here names a specific core.
 *
 * <p>One {@code ClassTemplate} is one playable class node in the profession tree: its canonical
 * identity ({@link #getClazz()}), the class it advances from ({@link #getParentClazz()}), and the
 * race / type / tier facets needed to group and lay out the tree. Identity is the
 * {@link CharacterClass} token only — there is no source-side numeric id (display names /
 * translations live consumer-side, keyed by the token).</p>
 *
 * <p><b>Nullability:</b> {@link #getClazz()} is non-null for any playable class (a non-canonical
 * fork class is a contract gap fixed by extending {@link CharacterClass}, not by emitting null).
 * {@link #getParentClazz()} is null for a base (root) class; the remaining facets are
 * {@link Nullable} so {@code null} means "this build did not supply it".</p>
 */
public final class ClassTemplate {

    private final @Nullable CharacterClass clazz;
    private final @Nullable CharacterClass parentClazz;
    private final @Nullable CharacterRace race;
    private final @Nullable CharacterClassType type;
    private final @Nullable CharacterClassTier tier;

    public ClassTemplate(
            @Nullable CharacterClass clazz,
            @Nullable CharacterClass parentClazz,
            @Nullable CharacterRace race,
            @Nullable CharacterClassType type,
            @Nullable CharacterClassTier tier) {
        this.clazz = clazz;
        this.parentClazz = parentClazz;
        this.race = race;
        this.type = type;
        this.tier = tier;
    }

    /**
     * Canonical class identity; non-null for a playable class.
     */
    public @Nullable CharacterClass getClazz() {
        return clazz;
    }

    /**
     * The class this one advances from (its profession parent); {@code null} for a base class.
     */
    public @Nullable CharacterClass getParentClazz() {
        return parentClazz;
    }

    /**
     * Race the class belongs to; {@code null} if unknown.
     */
    public @Nullable CharacterRace getRace() {
        return race;
    }

    /**
     * Whether the class is a fighter or a mystic; {@code null} if unknown.
     */
    public @Nullable CharacterClassType getType() {
        return type;
    }

    /**
     * Profession tier ({@code BASE} → {@code THIRD}); {@code null} if unknown.
     */
    public @Nullable CharacterClassTier getTier() {
        return tier;
    }

    public Builder toBuilder() {
        return new Builder()
                .clazz(clazz)
                .parentClazz(parentClazz)
                .race(race)
                .type(type)
                .tier(tier);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClassTemplate)) return false;
        ClassTemplate that = (ClassTemplate) o;
        return clazz == that.clazz
                && parentClazz == that.parentClazz
                && race == that.race
                && type == that.type
                && tier == that.tier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, parentClazz, race, type, tier);
    }

    @Override
    public String toString() {
        return "ClassTemplate[clazz=" + clazz + ", race=" + race + ", tier=" + tier + "]";
    }

    public static final class Builder {
        private @Nullable CharacterClass clazz;
        private @Nullable CharacterClass parentClazz;
        private @Nullable CharacterRace race;
        private @Nullable CharacterClassType type;
        private @Nullable CharacterClassTier tier;

        public Builder clazz(@Nullable CharacterClass clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder parentClazz(@Nullable CharacterClass parentClazz) {
            this.parentClazz = parentClazz;
            return this;
        }

        public Builder race(@Nullable CharacterRace race) {
            this.race = race;
            return this;
        }

        public Builder type(@Nullable CharacterClassType type) {
            this.type = type;
            return this;
        }

        public Builder tier(@Nullable CharacterClassTier tier) {
            this.tier = tier;
            return this;
        }

        public ClassTemplate build() {
            return new ClassTemplate(clazz, parentClazz, race, type, tier);
        }
    }
}
