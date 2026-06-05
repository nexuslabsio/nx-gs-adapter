package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import org.jspecify.annotations.Nullable;

import java.util.Map;
import java.util.Objects;

/**
 * Combat stats block of an {@link ItemTemplate} — the gameplay numbers that make a
 * weapon a weapon / armor armor. Carried as a nested object so etc-items (no combat
 * profile) simply omit it ({@code stats == null}).
 *
 * <p>All fields are {@link Nullable}: a given stat is present only when the item's
 * datapack template defines it (weapons carry the offensive set, armor the
 * defensive set). {@code durability} is intentionally absent here — it is a
 * client-patch field (Phase 2), not readable from the server's in-memory template.</p>
 *
 * <p>Server-side / memory-sourced: {@code pAtk}/{@code mAtk}/{@code pDef}/
 * {@code mDef}/{@code attackSpeed}/{@code criticalRate} come from the item's stat
 * functions; {@code attackRange}/{@code randomDamage}/{@code soulshots}/
 * {@code spiritshots}/{@code mpConsume}/{@code magicWeapon} from direct weapon
 * accessors. The provider maps its core's representation into these.</p>
 */
public final class ItemStats {

    private final @Nullable Integer pAtk;
    private final @Nullable Integer mAtk;
    private final @Nullable Integer pDef;
    private final @Nullable Integer mDef;
    private final @Nullable Integer attackRange;
    private final @Nullable Integer attackSpeed;
    private final @Nullable Integer criticalRate;
    private final @Nullable Integer randomDamage;
    private final @Nullable Integer soulshots;
    private final @Nullable Integer spiritshots;
    private final @Nullable Integer mpConsume;
    private final @Nullable Boolean magicWeapon;
    private final @Nullable Map<String, Double> statBonuses;

    private ItemStats(Builder b) {
        this.pAtk = b.pAtk;
        this.mAtk = b.mAtk;
        this.pDef = b.pDef;
        this.mDef = b.mDef;
        this.attackRange = b.attackRange;
        this.attackSpeed = b.attackSpeed;
        this.criticalRate = b.criticalRate;
        this.randomDamage = b.randomDamage;
        this.soulshots = b.soulshots;
        this.spiritshots = b.spiritshots;
        this.mpConsume = b.mpConsume;
        this.magicWeapon = b.magicWeapon;
        this.statBonuses = b.statBonuses;
    }

    public @Nullable Integer getPAtk() {
        return pAtk;
    }

    public @Nullable Integer getMAtk() {
        return mAtk;
    }

    public @Nullable Integer getPDef() {
        return pDef;
    }

    public @Nullable Integer getMDef() {
        return mDef;
    }

    public @Nullable Integer getAttackRange() {
        return attackRange;
    }

    public @Nullable Integer getAttackSpeed() {
        return attackSpeed;
    }

    public @Nullable Integer getCriticalRate() {
        return criticalRate;
    }

    public @Nullable Integer getRandomDamage() {
        return randomDamage;
    }

    public @Nullable Integer getSoulshots() {
        return soulshots;
    }

    public @Nullable Integer getSpiritshots() {
        return spiritshots;
    }

    public @Nullable Integer getMpConsume() {
        return mpConsume;
    }

    public @Nullable Boolean getMagicWeapon() {
        return magicWeapon;
    }

    /**
     * All stat bonuses the item grants, keyed by the canonical stat name
     * (e.g. {@code MAX_HP}, {@code STAT_STR}, {@code MAGIC_ATTACK}). The open
     * superset of the fixed combat fields above — carries the bonuses (mp bonus,
     * base-stat bonuses on jewelry, …) that have no dedicated field. {@code null}
     * if the item grants none.
     */
    public @Nullable Map<String, Double> getStatBonuses() {
        return statBonuses;
    }

    public Builder toBuilder() {
        return new Builder()
                .pAtk(pAtk)
                .mAtk(mAtk)
                .pDef(pDef)
                .mDef(mDef)
                .attackRange(attackRange)
                .attackSpeed(attackSpeed)
                .criticalRate(criticalRate)
                .randomDamage(randomDamage)
                .soulshots(soulshots)
                .spiritshots(spiritshots)
                .mpConsume(mpConsume)
                .magicWeapon(magicWeapon)
                .statBonuses(statBonuses);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemStats)) return false;
        ItemStats that = (ItemStats) o;
        return Objects.equals(pAtk, that.pAtk)
                && Objects.equals(mAtk, that.mAtk)
                && Objects.equals(pDef, that.pDef)
                && Objects.equals(mDef, that.mDef)
                && Objects.equals(attackRange, that.attackRange)
                && Objects.equals(attackSpeed, that.attackSpeed)
                && Objects.equals(criticalRate, that.criticalRate)
                && Objects.equals(randomDamage, that.randomDamage)
                && Objects.equals(soulshots, that.soulshots)
                && Objects.equals(spiritshots, that.spiritshots)
                && Objects.equals(mpConsume, that.mpConsume)
                && Objects.equals(magicWeapon, that.magicWeapon)
                && Objects.equals(statBonuses, that.statBonuses);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pAtk, mAtk, pDef, mDef, attackRange, attackSpeed, criticalRate,
                randomDamage, soulshots, spiritshots, mpConsume, magicWeapon, statBonuses);
    }

    @Override
    public String toString() {
        return "ItemStats[pAtk=" + pAtk + ", mAtk=" + mAtk + ", pDef=" + pDef + ", mDef=" + mDef + "]";
    }

    public static final class Builder {
        private @Nullable Integer pAtk;
        private @Nullable Integer mAtk;
        private @Nullable Integer pDef;
        private @Nullable Integer mDef;
        private @Nullable Integer attackRange;
        private @Nullable Integer attackSpeed;
        private @Nullable Integer criticalRate;
        private @Nullable Integer randomDamage;
        private @Nullable Integer soulshots;
        private @Nullable Integer spiritshots;
        private @Nullable Integer mpConsume;
        private @Nullable Boolean magicWeapon;
        private @Nullable Map<String, Double> statBonuses;

        public Builder pAtk(@Nullable Integer pAtk) {
            this.pAtk = pAtk;
            return this;
        }

        public Builder mAtk(@Nullable Integer mAtk) {
            this.mAtk = mAtk;
            return this;
        }

        public Builder pDef(@Nullable Integer pDef) {
            this.pDef = pDef;
            return this;
        }

        public Builder mDef(@Nullable Integer mDef) {
            this.mDef = mDef;
            return this;
        }

        public Builder attackRange(@Nullable Integer attackRange) {
            this.attackRange = attackRange;
            return this;
        }

        public Builder attackSpeed(@Nullable Integer attackSpeed) {
            this.attackSpeed = attackSpeed;
            return this;
        }

        public Builder criticalRate(@Nullable Integer criticalRate) {
            this.criticalRate = criticalRate;
            return this;
        }

        public Builder randomDamage(@Nullable Integer randomDamage) {
            this.randomDamage = randomDamage;
            return this;
        }

        public Builder soulshots(@Nullable Integer soulshots) {
            this.soulshots = soulshots;
            return this;
        }

        public Builder spiritshots(@Nullable Integer spiritshots) {
            this.spiritshots = spiritshots;
            return this;
        }

        public Builder mpConsume(@Nullable Integer mpConsume) {
            this.mpConsume = mpConsume;
            return this;
        }

        public Builder magicWeapon(@Nullable Boolean magicWeapon) {
            this.magicWeapon = magicWeapon;
            return this;
        }

        public Builder statBonuses(@Nullable Map<String, Double> statBonuses) {
            this.statBonuses = statBonuses;
            return this;
        }

        public ItemStats build() {
            return new ItemStats(this);
        }
    }
}
