package app.l2nx.gs.adapter.api.kafka.events.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Weekly "every weekday(s) at HH:MM" rule(s) for a tracked activity (boss respawn, siege, event), derived
 * by the host from its cron config. A {@code null} schedule on the entry means the activity is not
 * expressible as a weekly rule; the consumer then relies on the entry's next-occurrence instant alone.
 */
public final class RecurringSchedule {

    private final List<RecurringSlot> slots;

    public RecurringSchedule(@Nullable List<RecurringSlot> slots) {
        this.slots = slots == null
                ? Collections.<RecurringSlot>emptyList()
                : Collections.unmodifiableList(new ArrayList<RecurringSlot>(slots));
    }

    /** Never null, unmodifiable; a populated schedule has at least one slot. */
    public List<RecurringSlot> getSlots() {
        return slots;
    }

    public Builder toBuilder() {
        return new Builder().slots(slots);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RecurringSchedule)) return false;
        RecurringSchedule that = (RecurringSchedule) o;
        return Objects.equals(slots, that.slots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slots);
    }

    @Override
    public String toString() {
        return "RecurringSchedule[slots=" + slots + "]";
    }

    public static final class Builder {
        private @Nullable List<RecurringSlot> slots;

        public Builder slots(@Nullable List<RecurringSlot> slots) {
            this.slots = slots;
            return this;
        }

        public RecurringSchedule build() {
            return new RecurringSchedule(slots);
        }
    }
}
