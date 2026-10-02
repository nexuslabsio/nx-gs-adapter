package app.l2nx.gs.adapter.api.kafka.sync.db.announcement;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one {@code auto_announcements} row, payload of {@code SyncEvent<AutoAnnouncementDbDto>}.
 * Only {@code id} and {@code content} are required.
 */
public final class AutoAnnouncementDbDto {

    private final long id;
    private final String content;
    private final @Nullable Boolean critical;
    private final @Nullable Long initialMs;
    private final @Nullable Long delayMs;
    private final @Nullable Integer cycle;

    public AutoAnnouncementDbDto(
            long id,
            String content,
            @Nullable Boolean critical,
            @Nullable Long initialMs,
            @Nullable Long delayMs,
            @Nullable Integer cycle) {
        this.id = id;
        this.content = Objects.requireNonNull(content, "AutoAnnouncementDbDto.content is required");
        this.critical = critical;
        this.initialMs = initialMs;
        this.delayMs = delayMs;
        this.cycle = cycle;
    }

    /** Same value a {@link app.l2nx.gs.adapter.api.kafka.commands.announcement.DeleteAutoAnnouncementCommand} targets. */
    public long getId() {
        return id;
    }

    /**
     * Already in the platform neutral chat micro-format (literal {@code
     * }, bare URLs); host tokens are translated by the provider.
     */
    public String getContent() {
        return content;
    }

    /** True for the critical/alert channel. */
    public @Nullable Boolean getCritical() {
        return critical;
    }

    /** Delay from server start before the first broadcast. */
    public @Nullable Long getInitialMs() {
        return initialMs;
    }

    /** Repeat period between broadcasts. */
    public @Nullable Long getDelayMs() {
        return delayMs;
    }

    /** Host-defined repeat count, including any "infinite" sentinel. */
    public @Nullable Integer getCycle() {
        return cycle;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .content(content)
                .critical(critical)
                .initialMs(initialMs)
                .delayMs(delayMs)
                .cycle(cycle);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AutoAnnouncementDbDto)) return false;
        AutoAnnouncementDbDto that = (AutoAnnouncementDbDto) o;
        return id == that.id
                && content.equals(that.content)
                && Objects.equals(critical, that.critical)
                && Objects.equals(initialMs, that.initialMs)
                && Objects.equals(delayMs, that.delayMs)
                && Objects.equals(cycle, that.cycle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, content, critical, initialMs, delayMs, cycle);
    }

    @Override
    public String toString() {
        return "AutoAnnouncementDbDto[id=" + id
                + ", content=" + content
                + ", critical=" + critical
                + ", initialMs=" + initialMs
                + ", delayMs=" + delayMs
                + ", cycle=" + cycle + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable String content;
        private @Nullable Boolean critical;
        private @Nullable Long initialMs;
        private @Nullable Long delayMs;
        private @Nullable Integer cycle;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder content(String content) {
            this.content = content;
            return this;
        }

        public Builder critical(@Nullable Boolean critical) {
            this.critical = critical;
            return this;
        }

        public Builder initialMs(@Nullable Long initialMs) {
            this.initialMs = initialMs;
            return this;
        }

        public Builder delayMs(@Nullable Long delayMs) {
            this.delayMs = delayMs;
            return this;
        }

        public Builder cycle(@Nullable Integer cycle) {
            this.cycle = cycle;
            return this;
        }

        public AutoAnnouncementDbDto build() {
            return new AutoAnnouncementDbDto(id, content, critical, initialMs, delayMs, cycle);
        }
    }
}
