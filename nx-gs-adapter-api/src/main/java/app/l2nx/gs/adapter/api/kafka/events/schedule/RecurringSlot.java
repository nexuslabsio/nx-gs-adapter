package app.l2nx.gs.adapter.api.kafka.events.schedule;

import java.time.DayOfWeek;
import java.time.OffsetTime;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * One rule of a {@link RecurringSchedule}: weekdays plus a time-of-day. An everyday rule carries all seven
 * days; days serialize as {@link DayOfWeek} names.
 *
 * <p>{@link OffsetTime}, not {@link java.time.Instant}: a slot is a wall-clock time, not a moment. The
 * offset is the game server's UTC offset at publish time so the platform can normalize to UTC; this is the
 * one exception to the UTC-{@code Instant}-only wire rule. {@code jitterMinutes} is a randomization window
 * around {@code time} ({@code 0} = exact).</p>
 */
public final class RecurringSlot {

    private final Set<DayOfWeek> daysOfWeek;
    private final @Nullable OffsetTime time;
    private final int jitterMinutes;

    public RecurringSlot(@Nullable Set<DayOfWeek> daysOfWeek, @Nullable OffsetTime time, int jitterMinutes) {
        this.daysOfWeek = daysOfWeek == null || daysOfWeek.isEmpty()
                ? Collections.<DayOfWeek>emptySet()
                : Collections.unmodifiableSet(EnumSet.copyOf(daysOfWeek));
        this.time = time;
        this.jitterMinutes = jitterMinutes;
    }

    public Set<DayOfWeek> getDaysOfWeek() {
        return daysOfWeek;
    }

    public @Nullable OffsetTime getTime() {
        return time;
    }

    public int getJitterMinutes() {
        return jitterMinutes;
    }

    public Builder toBuilder() {
        return new Builder().daysOfWeek(daysOfWeek).time(time).jitterMinutes(jitterMinutes);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecurringSlot)) return false;
        RecurringSlot that = (RecurringSlot) o;
        return jitterMinutes == that.jitterMinutes
                && Objects.equals(daysOfWeek, that.daysOfWeek)
                && Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {
        return Objects.hash(daysOfWeek, time, jitterMinutes);
    }

    @Override
    public String toString() {
        return "RecurringSlot[daysOfWeek=" + daysOfWeek + ", time=" + time + ", jitterMinutes=" + jitterMinutes + "]";
    }

    public static final class Builder {
        private @Nullable Set<DayOfWeek> daysOfWeek;
        private @Nullable OffsetTime time;
        private int jitterMinutes;

        public Builder daysOfWeek(@Nullable Set<DayOfWeek> daysOfWeek) {
            this.daysOfWeek = daysOfWeek;
            return this;
        }

        public Builder time(@Nullable OffsetTime time) {
            this.time = time;
            return this;
        }

        public Builder jitterMinutes(int jitterMinutes) {
            this.jitterMinutes = jitterMinutes;
            return this;
        }

        public RecurringSlot build() {
            return new RecurringSlot(daysOfWeek, time, jitterMinutes);
        }
    }
}
