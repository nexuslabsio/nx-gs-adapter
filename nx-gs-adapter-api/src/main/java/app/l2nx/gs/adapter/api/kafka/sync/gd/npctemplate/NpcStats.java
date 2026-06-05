package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Combat / progression stat block of an {@link NpcTemplate} — base HP/MP, regen,
 * attack & defence, speeds, and kill rewards. Carried as a nested object; all
 * fields {@link Nullable} (present only when the build's template defines them).
 *
 * <p>Every value comes from a typed accessor on the host's in-memory template
 * (the raw {@code <set>} parameter map the datapack parses into is discarded after
 * build, so values are read field-by-field, not as a generic map). {@code rewardExp}/
 * {@code rewardSp} are {@code Long}; {@code rewardRp} (raid / reputation points) is
 * {@code Integer}.</p>
 */
public final class NpcStats {

    private final @Nullable Double baseHpMax;
    private final @Nullable Double baseMpMax;
    private final @Nullable Double baseHpReg;
    private final @Nullable Double baseMpReg;
    private final @Nullable Double basePAtk;
    private final @Nullable Double basePDef;
    private final @Nullable Double baseMAtk;
    private final @Nullable Double baseMDef;
    private final @Nullable Integer basePAtkSpd;
    private final @Nullable Integer baseMAtkSpd;
    private final @Nullable Integer baseRunSpd;
    private final @Nullable Integer baseWalkSpd;
    private final @Nullable Integer baseAtkRange;
    private final @Nullable Integer baseCritRate;
    private final @Nullable Integer baseShldDef;
    private final @Nullable Integer baseShldRate;
    private final @Nullable Long rewardExp;
    private final @Nullable Long rewardSp;
    private final @Nullable Integer rewardRp;

    private NpcStats(Builder b) {
        this.baseHpMax = b.baseHpMax;
        this.baseMpMax = b.baseMpMax;
        this.baseHpReg = b.baseHpReg;
        this.baseMpReg = b.baseMpReg;
        this.basePAtk = b.basePAtk;
        this.basePDef = b.basePDef;
        this.baseMAtk = b.baseMAtk;
        this.baseMDef = b.baseMDef;
        this.basePAtkSpd = b.basePAtkSpd;
        this.baseMAtkSpd = b.baseMAtkSpd;
        this.baseRunSpd = b.baseRunSpd;
        this.baseWalkSpd = b.baseWalkSpd;
        this.baseAtkRange = b.baseAtkRange;
        this.baseCritRate = b.baseCritRate;
        this.baseShldDef = b.baseShldDef;
        this.baseShldRate = b.baseShldRate;
        this.rewardExp = b.rewardExp;
        this.rewardSp = b.rewardSp;
        this.rewardRp = b.rewardRp;
    }

    public @Nullable Double getBaseHpMax() {
        return baseHpMax;
    }

    public @Nullable Double getBaseMpMax() {
        return baseMpMax;
    }

    public @Nullable Double getBaseHpReg() {
        return baseHpReg;
    }

    public @Nullable Double getBaseMpReg() {
        return baseMpReg;
    }

    public @Nullable Double getBasePAtk() {
        return basePAtk;
    }

    public @Nullable Double getBasePDef() {
        return basePDef;
    }

    public @Nullable Double getBaseMAtk() {
        return baseMAtk;
    }

    public @Nullable Double getBaseMDef() {
        return baseMDef;
    }

    public @Nullable Integer getBasePAtkSpd() {
        return basePAtkSpd;
    }

    public @Nullable Integer getBaseMAtkSpd() {
        return baseMAtkSpd;
    }

    public @Nullable Integer getBaseRunSpd() {
        return baseRunSpd;
    }

    public @Nullable Integer getBaseWalkSpd() {
        return baseWalkSpd;
    }

    public @Nullable Integer getBaseAtkRange() {
        return baseAtkRange;
    }

    public @Nullable Integer getBaseCritRate() {
        return baseCritRate;
    }

    public @Nullable Integer getBaseShldDef() {
        return baseShldDef;
    }

    public @Nullable Integer getBaseShldRate() {
        return baseShldRate;
    }

    public @Nullable Long getRewardExp() {
        return rewardExp;
    }

    public @Nullable Long getRewardSp() {
        return rewardSp;
    }

    public @Nullable Integer getRewardRp() {
        return rewardRp;
    }

    public Builder toBuilder() {
        return new Builder()
                .baseHpMax(baseHpMax)
                .baseMpMax(baseMpMax)
                .baseHpReg(baseHpReg)
                .baseMpReg(baseMpReg)
                .basePAtk(basePAtk)
                .basePDef(basePDef)
                .baseMAtk(baseMAtk)
                .baseMDef(baseMDef)
                .basePAtkSpd(basePAtkSpd)
                .baseMAtkSpd(baseMAtkSpd)
                .baseRunSpd(baseRunSpd)
                .baseWalkSpd(baseWalkSpd)
                .baseAtkRange(baseAtkRange)
                .baseCritRate(baseCritRate)
                .baseShldDef(baseShldDef)
                .baseShldRate(baseShldRate)
                .rewardExp(rewardExp)
                .rewardSp(rewardSp)
                .rewardRp(rewardRp);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof NpcStats)) return false;
        NpcStats that = (NpcStats) o;
        return Objects.equals(baseHpMax, that.baseHpMax)
                && Objects.equals(baseMpMax, that.baseMpMax)
                && Objects.equals(baseHpReg, that.baseHpReg)
                && Objects.equals(baseMpReg, that.baseMpReg)
                && Objects.equals(basePAtk, that.basePAtk)
                && Objects.equals(basePDef, that.basePDef)
                && Objects.equals(baseMAtk, that.baseMAtk)
                && Objects.equals(baseMDef, that.baseMDef)
                && Objects.equals(basePAtkSpd, that.basePAtkSpd)
                && Objects.equals(baseMAtkSpd, that.baseMAtkSpd)
                && Objects.equals(baseRunSpd, that.baseRunSpd)
                && Objects.equals(baseWalkSpd, that.baseWalkSpd)
                && Objects.equals(baseAtkRange, that.baseAtkRange)
                && Objects.equals(baseCritRate, that.baseCritRate)
                && Objects.equals(baseShldDef, that.baseShldDef)
                && Objects.equals(baseShldRate, that.baseShldRate)
                && Objects.equals(rewardExp, that.rewardExp)
                && Objects.equals(rewardSp, that.rewardSp)
                && Objects.equals(rewardRp, that.rewardRp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(baseHpMax, baseMpMax, baseHpReg, baseMpReg, basePAtk, basePDef,
                baseMAtk, baseMDef, basePAtkSpd, baseMAtkSpd, baseRunSpd, baseWalkSpd, baseAtkRange,
                baseCritRate, baseShldDef, baseShldRate, rewardExp, rewardSp, rewardRp);
    }

    @Override
    public String toString() {
        return "NpcStats[baseHpMax=" + baseHpMax + ", basePAtk=" + basePAtk + ", baseMAtk=" + baseMAtk + "]";
    }

    public static final class Builder {
        private @Nullable Double baseHpMax;
        private @Nullable Double baseMpMax;
        private @Nullable Double baseHpReg;
        private @Nullable Double baseMpReg;
        private @Nullable Double basePAtk;
        private @Nullable Double basePDef;
        private @Nullable Double baseMAtk;
        private @Nullable Double baseMDef;
        private @Nullable Integer basePAtkSpd;
        private @Nullable Integer baseMAtkSpd;
        private @Nullable Integer baseRunSpd;
        private @Nullable Integer baseWalkSpd;
        private @Nullable Integer baseAtkRange;
        private @Nullable Integer baseCritRate;
        private @Nullable Integer baseShldDef;
        private @Nullable Integer baseShldRate;
        private @Nullable Long rewardExp;
        private @Nullable Long rewardSp;
        private @Nullable Integer rewardRp;

        public Builder baseHpMax(@Nullable Double baseHpMax) {
            this.baseHpMax = baseHpMax;
            return this;
        }

        public Builder baseMpMax(@Nullable Double baseMpMax) {
            this.baseMpMax = baseMpMax;
            return this;
        }

        public Builder baseHpReg(@Nullable Double baseHpReg) {
            this.baseHpReg = baseHpReg;
            return this;
        }

        public Builder baseMpReg(@Nullable Double baseMpReg) {
            this.baseMpReg = baseMpReg;
            return this;
        }

        public Builder basePAtk(@Nullable Double basePAtk) {
            this.basePAtk = basePAtk;
            return this;
        }

        public Builder basePDef(@Nullable Double basePDef) {
            this.basePDef = basePDef;
            return this;
        }

        public Builder baseMAtk(@Nullable Double baseMAtk) {
            this.baseMAtk = baseMAtk;
            return this;
        }

        public Builder baseMDef(@Nullable Double baseMDef) {
            this.baseMDef = baseMDef;
            return this;
        }

        public Builder basePAtkSpd(@Nullable Integer basePAtkSpd) {
            this.basePAtkSpd = basePAtkSpd;
            return this;
        }

        public Builder baseMAtkSpd(@Nullable Integer baseMAtkSpd) {
            this.baseMAtkSpd = baseMAtkSpd;
            return this;
        }

        public Builder baseRunSpd(@Nullable Integer baseRunSpd) {
            this.baseRunSpd = baseRunSpd;
            return this;
        }

        public Builder baseWalkSpd(@Nullable Integer baseWalkSpd) {
            this.baseWalkSpd = baseWalkSpd;
            return this;
        }

        public Builder baseAtkRange(@Nullable Integer baseAtkRange) {
            this.baseAtkRange = baseAtkRange;
            return this;
        }

        public Builder baseCritRate(@Nullable Integer baseCritRate) {
            this.baseCritRate = baseCritRate;
            return this;
        }

        public Builder baseShldDef(@Nullable Integer baseShldDef) {
            this.baseShldDef = baseShldDef;
            return this;
        }

        public Builder baseShldRate(@Nullable Integer baseShldRate) {
            this.baseShldRate = baseShldRate;
            return this;
        }

        public Builder rewardExp(@Nullable Long rewardExp) {
            this.rewardExp = rewardExp;
            return this;
        }

        public Builder rewardSp(@Nullable Long rewardSp) {
            this.rewardSp = rewardSp;
            return this;
        }

        public Builder rewardRp(@Nullable Integer rewardRp) {
            this.rewardRp = rewardRp;
            return this;
        }

        public NpcStats build() {
            return new NpcStats(this);
        }
    }
}
