package app.l2nx.gs.adapter.api.kafka.sync.gd.skilltemplate;

import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * One stat modification a skill (or one of its effects) applies — the build-agnostic
 * projection of a core function template ({@code <add>} / {@code <mul>} / … nodes in
 * L2J-family datapacks). This is the substance of passive skills and buffs: which stat
 * is modified, how, and by how much.
 *
 * <p>{@code stat} is the canonical UPPER_SNAKE stat token (the core stat-enum name, e.g.
 * {@code POWER_ATTACK}, {@code MAX_HP}, {@code MOVE_SPEED}) — same vocabulary as item
 * stat bonuses. {@code op} is the canonical UPPER_SNAKE operation token derived from the
 * core function kind ({@code ADD} / {@code SUB} / {@code MUL} / {@code BASE_MUL} /
 * {@code DIV} / {@code SET} / {@code SHARE} / {@code ENCHANT} / {@code ENCHANT_ADD} /
 * {@code ENCHANT_HP} / {@code ENCHANT_MUL} / {@code GET}). Both are non-null identity.</p>
 *
 * <p>{@code value} is the constant operand; {@code null} when the core computes the
 * operand dynamically (a formula over caster state) — the modifier still names the stat
 * and operation, only the magnitude is runtime-dependent. {@code order} is the core's
 * application-order byte (modifiers of the same stat apply in ascending order);
 * {@code null} when not supplied.</p>
 */
public final class SkillStatModifier {

    private final String stat;
    private final String op;
    private final @Nullable Double value;
    private final @Nullable Integer order;

    public SkillStatModifier(String stat,
                             String op,
                             @Nullable Double value,
                             @Nullable Integer order) {
        this.stat = Objects.requireNonNull(stat, "stat");
        this.op = Objects.requireNonNull(op, "op");
        this.value = value;
        this.order = order;
    }

    /**
     * Canonical UPPER_SNAKE stat token (core stat-enum name).
     */
    public String getStat() {
        return stat;
    }

    /**
     * Canonical UPPER_SNAKE operation token (core function kind).
     */
    public String getOp() {
        return op;
    }

    /**
     * Constant operand; {@code null} when the core computes it dynamically.
     */
    public @Nullable Double getValue() {
        return value;
    }

    /**
     * Application order among modifiers of the same stat (ascending); {@code null} if
     * not supplied.
     */
    public @Nullable Integer getOrder() {
        return order;
    }

    public Builder toBuilder() {
        return new Builder()
                .stat(stat)
                .op(op)
                .value(value)
                .order(order);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillStatModifier)) return false;
        SkillStatModifier that = (SkillStatModifier) o;
        return Objects.equals(stat, that.stat)
                && Objects.equals(op, that.op)
                && Objects.equals(value, that.value)
                && Objects.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stat, op, value, order);
    }

    @Override
    public String toString() {
        return "SkillStatModifier[stat=" + stat + ", op=" + op + ", value=" + value + "]";
    }

    public static final class Builder {
        private String stat;
        private String op;
        private @Nullable Double value;
        private @Nullable Integer order;

        public Builder stat(String stat) {
            this.stat = stat;
            return this;
        }

        public Builder op(String op) {
            this.op = op;
            return this;
        }

        public Builder value(@Nullable Double value) {
            this.value = value;
            return this;
        }

        public Builder order(@Nullable Integer order) {
            this.order = order;
            return this;
        }

        public SkillStatModifier build() {
            return new SkillStatModifier(stat, op, value, order);
        }
    }
}
