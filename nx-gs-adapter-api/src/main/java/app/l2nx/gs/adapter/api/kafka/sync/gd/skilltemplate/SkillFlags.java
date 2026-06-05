package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Cohesive cluster of a {@link SkillTemplate}'s boolean classification flags — "what kind of
 * skill is this". Grouped (the same way {@code ItemRestrictions} groups an item's
 * permission flags) so the header surface is one object instead of a scatter of
 * booleans; the consumer unwraps it flat into queryable columns.
 *
 * <p>All fields are tri-state {@link Nullable Boolean} ({@code true}/{@code false}/
 * unknown) and carry no {@code is}-prefix (gd-DTO convention — accessor name is
 * {@code getMagic()} etc.). These flags are level-invariant; the provider reads them
 * from the skill's canonical level.</p>
 */
public final class SkillFlags {

    private final @Nullable Boolean magic;
    private final @Nullable Boolean debuff;
    private final @Nullable Boolean offensive;
    private final @Nullable Boolean passive;
    private final @Nullable Boolean toggle;
    private final @Nullable Boolean staticSkill;
    private final @Nullable Boolean blockedInOlympiad;
    private final @Nullable Boolean overHit;
    private final @Nullable Boolean ignoreShield;
    private final @Nullable Boolean nextActionAttack;

    public SkillFlags(@Nullable Boolean magic,
                      @Nullable Boolean debuff,
                      @Nullable Boolean offensive,
                      @Nullable Boolean passive,
                      @Nullable Boolean toggle,
                      @Nullable Boolean staticSkill,
                      @Nullable Boolean blockedInOlympiad,
                      @Nullable Boolean overHit,
                      @Nullable Boolean ignoreShield,
                      @Nullable Boolean nextActionAttack) {
        this.magic = magic;
        this.debuff = debuff;
        this.offensive = offensive;
        this.passive = passive;
        this.toggle = toggle;
        this.staticSkill = staticSkill;
        this.blockedInOlympiad = blockedInOlympiad;
        this.overHit = overHit;
        this.ignoreShield = ignoreShield;
        this.nextActionAttack = nextActionAttack;
    }

    /**
     * Magic skill (vs physical) — drives m.def mitigation and cast interruption rules.
     */
    public @Nullable Boolean getMagic() {
        return magic;
    }

    public @Nullable Boolean getDebuff() {
        return debuff;
    }

    /**
     * Hostile skill (targets enemies) — drives flagging / PvP rules.
     */
    public @Nullable Boolean getOffensive() {
        return offensive;
    }

    public @Nullable Boolean getPassive() {
        return passive;
    }

    public @Nullable Boolean getToggle() {
        return toggle;
    }

    /**
     * Static skill — does not scale with the caster's stats. Field is
     * {@code staticSkill} ({@code static} is a Java keyword); DB column {@code static_skill}.
     */
    public @Nullable Boolean getStaticSkill() {
        return staticSkill;
    }

    public @Nullable Boolean getBlockedInOlympiad() {
        return blockedInOlympiad;
    }

    /**
     * Can over-hit (excess damage carries to exp reward on the killing blow).
     */
    public @Nullable Boolean getOverHit() {
        return overHit;
    }

    public @Nullable Boolean getIgnoreShield() {
        return ignoreShield;
    }

    /**
     * Whether the character auto-attacks the target after this skill resolves.
     */
    public @Nullable Boolean getNextActionAttack() {
        return nextActionAttack;
    }

    public Builder toBuilder() {
        return new Builder()
                .magic(magic)
                .debuff(debuff)
                .offensive(offensive)
                .passive(passive)
                .toggle(toggle)
                .staticSkill(staticSkill)
                .blockedInOlympiad(blockedInOlympiad)
                .overHit(overHit)
                .ignoreShield(ignoreShield)
                .nextActionAttack(nextActionAttack);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillFlags)) return false;
        SkillFlags that = (SkillFlags) o;
        return Objects.equals(magic, that.magic)
                && Objects.equals(debuff, that.debuff)
                && Objects.equals(offensive, that.offensive)
                && Objects.equals(passive, that.passive)
                && Objects.equals(toggle, that.toggle)
                && Objects.equals(staticSkill, that.staticSkill)
                && Objects.equals(blockedInOlympiad, that.blockedInOlympiad)
                && Objects.equals(overHit, that.overHit)
                && Objects.equals(ignoreShield, that.ignoreShield)
                && Objects.equals(nextActionAttack, that.nextActionAttack);
    }

    @Override
    public int hashCode() {
        return Objects.hash(magic, debuff, offensive, passive, toggle, staticSkill,
                blockedInOlympiad, overHit, ignoreShield, nextActionAttack);
    }

    @Override
    public String toString() {
        return "SkillFlags[magic=" + magic + ", debuff=" + debuff + ", passive=" + passive + "]";
    }

    public static final class Builder {
        private @Nullable Boolean magic;
        private @Nullable Boolean debuff;
        private @Nullable Boolean offensive;
        private @Nullable Boolean passive;
        private @Nullable Boolean toggle;
        private @Nullable Boolean staticSkill;
        private @Nullable Boolean blockedInOlympiad;
        private @Nullable Boolean overHit;
        private @Nullable Boolean ignoreShield;
        private @Nullable Boolean nextActionAttack;

        public Builder magic(@Nullable Boolean magic) {
            this.magic = magic;
            return this;
        }

        public Builder debuff(@Nullable Boolean debuff) {
            this.debuff = debuff;
            return this;
        }

        public Builder offensive(@Nullable Boolean offensive) {
            this.offensive = offensive;
            return this;
        }

        public Builder passive(@Nullable Boolean passive) {
            this.passive = passive;
            return this;
        }

        public Builder toggle(@Nullable Boolean toggle) {
            this.toggle = toggle;
            return this;
        }

        public Builder staticSkill(@Nullable Boolean staticSkill) {
            this.staticSkill = staticSkill;
            return this;
        }

        public Builder blockedInOlympiad(@Nullable Boolean blockedInOlympiad) {
            this.blockedInOlympiad = blockedInOlympiad;
            return this;
        }

        public Builder overHit(@Nullable Boolean overHit) {
            this.overHit = overHit;
            return this;
        }

        public Builder ignoreShield(@Nullable Boolean ignoreShield) {
            this.ignoreShield = ignoreShield;
            return this;
        }

        public Builder nextActionAttack(@Nullable Boolean nextActionAttack) {
            this.nextActionAttack = nextActionAttack;
            return this;
        }

        public SkillFlags build() {
            return new SkillFlags(magic, debuff, offensive, passive, toggle, staticSkill,
                    blockedInOlympiad, overHit, ignoreShield, nextActionAttack);
        }
    }
}
