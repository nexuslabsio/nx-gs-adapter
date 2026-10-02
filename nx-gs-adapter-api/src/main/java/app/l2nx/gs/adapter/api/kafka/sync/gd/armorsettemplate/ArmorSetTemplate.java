package app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one armor set, payload of {@code GameDataSyncEvent} on the {@code armorsettemplate} topic; the
 * consumer replaces children atomically. A slot may carry several alternative {@link ArmorSetItem} rows.
 */
public final class ArmorSetTemplate {

    private final int id;
    private final @Nullable ArmorSetStatBonus statBonus;
    private final @Nullable List<ArmorSetItem> items;
    private final @Nullable List<ArmorSetSkill> skills;

    public ArmorSetTemplate(
            int id,
            @Nullable ArmorSetStatBonus statBonus,
            @Nullable List<ArmorSetItem> items,
            @Nullable List<ArmorSetSkill> skills) {
        this.id = id;
        this.statBonus = statBonus;
        this.items = items == null ? null : Collections.unmodifiableList(new ArrayList<ArmorSetItem>(items));
        this.skills = skills == null ? null : Collections.unmodifiableList(new ArrayList<ArmorSetSkill>(skills));
    }

    public int getId() {
        return id;
    }

    public @Nullable ArmorSetStatBonus getStatBonus() {
        return statBonus;
    }

    public @Nullable List<ArmorSetItem> getItems() {
        return items;
    }

    public @Nullable List<ArmorSetSkill> getSkills() {
        return skills;
    }

    public Builder toBuilder() {
        return new Builder().id(id).statBonus(statBonus).items(items).skills(skills);
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
