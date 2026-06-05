package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Elemental-attribute block of an {@link NpcTemplate} — the NPC's attack and defence
 * per element. "Attribute" is L2 business vocabulary for element (as with an item's
 * {@code attributable} flag); not to be confused with {@link NpcBaseAttributes}
 * (STR/DEX/…).
 *
 * <p>Each map is element-keyed by {@code UPPER_CASE} element name
 * ({@code FIRE}/{@code WATER}/{@code WIND}/{@code EARTH}/{@code HOLY}/{@code DARK}).
 * Both maps are {@link Nullable} and, when present, immutable.</p>
 */
public final class NpcAttribute {

    private final @Nullable Map<String, Integer> attack;
    private final @Nullable Map<String, Integer> defence;

    private NpcAttribute(Builder b) {
        this.attack = b.attack == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(b.attack));
        this.defence = b.defence == null ? null
                : Collections.unmodifiableMap(new LinkedHashMap<String, Integer>(b.defence));
    }

    public @Nullable Map<String, Integer> getAttack() {
        return attack;
    }

    public @Nullable Map<String, Integer> getDefence() {
        return defence;
    }

    public Builder toBuilder() {
        return new Builder()
                .attack(attack)
                .defence(defence);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcAttribute)) return false;
        NpcAttribute that = (NpcAttribute) o;
        return Objects.equals(attack, that.attack) && Objects.equals(defence, that.defence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(attack, defence);
    }

    @Override
    public String toString() {
        return "NpcAttribute[attack=" + attack + ", defence=" + defence + "]";
    }

    public static final class Builder {
        private @Nullable Map<String, Integer> attack;
        private @Nullable Map<String, Integer> defence;

        public Builder attack(@Nullable Map<String, Integer> attack) {
            this.attack = attack;
            return this;
        }

        public Builder defence(@Nullable Map<String, Integer> defence) {
            this.defence = defence;
            return this;
        }

        public NpcAttribute build() {
            return new NpcAttribute(this);
        }
    }
}
