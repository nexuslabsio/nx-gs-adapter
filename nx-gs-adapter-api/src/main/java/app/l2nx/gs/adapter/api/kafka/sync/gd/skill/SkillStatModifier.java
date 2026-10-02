package app.l2nx.gs.adapter.api.kafka.sync.gd.skill;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * One stat modification of a skill or effect, projected from a core function template. {@code stat} (core
 * stat-enum name, e.g. {@code MAX_HP}) and {@code op} (core function kind, e.g. {@code ADD}, {@code BASE_MUL})
 * are non-null UPPER_SNAKE tokens. {@code value} is {@code null} when the core computes the operand at runtime;
 * {@code order} is the ascending application order among modifiers of the same stat.
 */
public final class SkillStatModifier {

    private final String stat;
    private final String op;
    private final @Nullable Double value;
    private final @Nullable Integer order;

    public SkillStatModifier(String stat, String op, @Nullable Double value, @Nullable Integer order) {
        this.stat = Objects.requireNonNull(stat, "stat");
        this.op = Objects.requireNonNull(op, "op");
        this.value = value;
        this.order = order;
    }

    public String getStat() {
        return stat;
    }

    public String getOp() {
        return op;
    }

    public @Nullable Double getValue() {
        return value;
    }

    public @Nullable Integer getOrder() {
        return order;
    }

    public Builder toBuilder() {
        return new Builder().stat(stat).op(op).value(value).order(order);
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
