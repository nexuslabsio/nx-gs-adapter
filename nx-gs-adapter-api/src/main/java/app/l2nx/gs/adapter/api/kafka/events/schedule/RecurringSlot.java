package app.l2nx.gs.adapter.api.kafka.events.schedule;

import java.time.DayOfWeek;
import java.time.OffsetTime;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;
import org.jspecify.annotations.Nullable;

/**
 * One recurring occurrence rule inside a {@link RecurringSchedule}: a set of
 * weekdays plus a clock time-of-day at which the activity fires.
 *
 * <p>Fields:
 * <ul>
 *   <li>{@link #getDaysOfWeek() daysOfWeek} — REQUIRED. The weekdays this slot
 *   fires on; an everyday rule carries all seven. Serialized as the
 *   {@link DayOfWeek} names ({@code "MONDAY"} … {@code "SUNDAY"}).</li>
 *   <li>{@link #getTime() time} — REQUIRED. The clock time-of-day with the
 *   game server's UTC offset, e.g. {@code "22:00:00+03:00"}. See the note below
 *   on why this is an {@link OffsetTime} and not an {@link java.time.Instant}.</li>
 *   <li>{@link #getJitterMinutes() jitterMinutes} — randomization window in
 *   minutes around {@link #getTime() time} (e.g. an epic boss configured with a
 *   {@code "~"} respawn pattern); {@code 0} = the time is exact.</li>
 * </ul>
 *
 * <p><b>Why {@link OffsetTime}, not {@link java.time.Instant}.</b> A recurring
 * slot is a wall-clock time-of-day, not a single moment in time, so it cannot be
 * an {@code Instant}. The offset is carried deliberately: it is the game server's
 * UTC offset at publish time, so the platform can normalize the slot to UTC on
 * ingest. This is the one sanctioned exception to the "timestamps are UTC
 * {@code Instant} only" wire rule — it applies to instants, not to clock times.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
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

    /**
     * Weekdays this slot fires on (all seven for an everyday rule). Never
     * {@code null}; the returned set is unmodifiable.
     */
    public Set<DayOfWeek> getDaysOfWeek() {
        return daysOfWeek;
    }

    /**
     * Clock time-of-day with the game server's UTC offset, e.g.
     * {@code "22:00:00+03:00"}.
     */
    public @Nullable OffsetTime getTime() {
        return time;
    }

    /**
     * Randomization window in minutes around {@link #getTime()}; {@code 0} when
     * the time is exact.
     */
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
