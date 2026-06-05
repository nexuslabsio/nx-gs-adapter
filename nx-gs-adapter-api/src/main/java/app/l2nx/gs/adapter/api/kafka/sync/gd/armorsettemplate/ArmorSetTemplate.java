package app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Build-agnostic armor-set wire DTO — the common L2 denominator for a single set, carried
 * as the payload of {@code GameDataSyncEvent} on the {@code gd} (game-data) sync stream's
 * {@code armorsettemplate} entity topic. Each host build supplies its own provider that maps
 * its core's internal set representation into this shape; nothing here names a specific core.
 *
 * <p>One {@code ArmorSetTemplate} is the whole aggregate for a set id: the flat stat bonus
 * group ({@link #getStatBonus()}) plus the nested per-slot item list ({@link #getItems()})
 * and granted-skill list ({@link #getSkills()}). A slot can carry multiple alternative items
 * (each its own {@link ArmorSetItem} row). The consumer upserts the parent and replaces its
 * children atomically.</p>
 *
 * <p><b>Nullability:</b> only {@link #getId()} is non-null. Item references inside children
 * use the canonical {@code itemTemplateId} name; skill references use {@code skillTemplateId}.</p>
 */
public final class ArmorSetTemplate {

    private final int id;
    private final @Nullable ArmorSetStatBonus statBonus;
    private final @Nullable List<ArmorSetItem> items;
    private final @Nullable List<ArmorSetSkill> skills;

    public ArmorSetTemplate(int id,
                            @Nullable ArmorSetStatBonus statBonus,
                            @Nullable List<ArmorSetItem> items,
                            @Nullable List<ArmorSetSkill> skills) {
        this.id = id;
        this.statBonus = statBonus;
        this.items = items == null ? null
                : Collections.unmodifiableList(new ArrayList<ArmorSetItem>(items));
        this.skills = skills == null ? null
                : Collections.unmodifiableList(new ArrayList<ArmorSetSkill>(skills));
    }

    public int getId() {
        return id;
    }

    /**
     * Flat stat bonuses granted while the set is active; {@code null} if none supplied.
     */
    public @Nullable ArmorSetStatBonus getStatBonus() {
        return statBonus;
    }

    /**
     * Items composing the set, one row per (slot, item) — a slot may have several
     * alternative items. {@code null} if not supplied.
     */
    public @Nullable List<ArmorSetItem> getItems() {
        return items;
    }

    /**
     * Skills the set grants (base / shield / enchant6 / enchant-by-level); {@code null} if none.
     */
    public @Nullable List<ArmorSetSkill> getSkills() {
        return skills;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .statBonus(statBonus)
                .items(items)
                .skills(skills);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArmorSetTemplate)) return false;
        ArmorSetTemplate that = (ArmorSetTemplate) o;
        return id == that.id
                && Objects.equals(statBonus, that.statBonus)
                && Objects.equals(items, that.items)
                && Objects.equals(skills, that.skills);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, statBonus, items, skills);
    }

    @Override
    public String toString() {
        return "ArmorSetTemplate[id=" + id + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable ArmorSetStatBonus statBonus;
        private @Nullable List<ArmorSetItem> items;
        private @Nullable List<ArmorSetSkill> skills;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder statBonus(@Nullable ArmorSetStatBonus statBonus) {
            this.statBonus = statBonus;
            return this;
        }

        public Builder items(@Nullable List<ArmorSetItem> items) {
            this.items = items;
            return this;
        }

        public Builder skills(@Nullable List<ArmorSetSkill> skills) {
            this.skills = skills;
            return this;
        }

        public ArmorSetTemplate build() {
            return new ArmorSetTemplate(id, statBonus, items, skills);
        }
    }
}
