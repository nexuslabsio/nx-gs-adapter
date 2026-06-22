package app.l2nx.gs.adapter.api.kafka.events.schedule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * A recurring (weekly) schedule for a tracked activity — raid/epic boss respawn,
 * castle siege, or game event — carried as an optional field on the corresponding
 * snapshot entry. Describes the "every &lt;weekday(s)&gt; at HH:MM" rule(s) the
 * host derives from its own cron-style configuration, complementing the single
 * "next occurrence" instant already on the entry.
 *
 * <p>{@code null} schedule means the host could not express the activity as a
 * weekly rule (e.g. a one-off / seasonal cron, or a respawn-window boss) — the
 * consumer then relies on the entry's next-occurrence instant alone.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor parameter
 * names so Gson / Jackson can deserialize without {@code @JsonProperty}.</p>
 */
public final class RecurringSchedule {

    private final List<RecurringSlot> slots;

    public RecurringSchedule(@Nullable List<RecurringSlot> slots) {
        this.slots = slots == null
                ? Collections.<RecurringSlot>emptyList()
                : Collections.unmodifiableList(new ArrayList<RecurringSlot>(slots));
    }

    /**
     * The recurrence rules; never {@code null}, the returned list is unmodifiable.
     * A populated schedule carries at least one slot.
     */
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
