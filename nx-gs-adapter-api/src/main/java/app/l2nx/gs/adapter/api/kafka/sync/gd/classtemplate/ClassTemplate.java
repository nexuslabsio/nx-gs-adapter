package app.l2nx.gs.adapter.api.kafka.sync.gd.classtemplate;

import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassTier;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassType;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one playable-class node of the profession tree, payload of {@code GameDataSyncEvent} on the
 * {@code classtemplate} topic; identity is the {@link CharacterClass} token only, no numeric id. {@link #getClazz()} is
 * non-null (non-canonical fork classes extend {@link CharacterClass}); {@link #getParentClazz()} is null for a base class.
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

    public @Nullable CharacterClass getClazz() {
        return clazz;
    }

    public @Nullable CharacterClass getParentClazz() {
        return parentClazz;
    }

    public @Nullable CharacterRace getRace() {
        return race;
    }

    public @Nullable CharacterClassType getType() {
        return type;
    }

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
