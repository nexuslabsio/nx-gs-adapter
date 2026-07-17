package app.l2nx.gs.adapter.api.kafka.sync.db.announcement;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one persisted {@code auto_announcements} row, payload of
 * {@code SyncEvent<AutoAnnouncementDbDto>} on the per-tenant
 * {@code auto_announcement} db-sync topic. Mirrors the game-server's native
 * in-game announcement rows so the platform can list them read-only
 * alongside its own authored announcements (origin {@code GAME} vs
 * {@code L2NX} is a platform-side distinction, not carried on this DTO).
 *
 * <p>{@link #getContent() content} is already the platform's neutral chat
 * micro-format — the schema provider performs the host-specific
 * {@code /n} → {@code \n} / {@code [=url=]} → bare-URL translation in
 * {@code mapEntity} before this DTO is built, so consumers never see the
 * bohpts wire tokens here.</p>
 *
 * <p>Only {@link #getId() id} and {@link #getContent() content} are
 * required; the remaining fields mirror the host's native scheduling
 * columns and are optional so a host can surface whatever subset its schema
 * carries.</p>
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

    /**
     * Primary key — the host's native {@code auto_announcements} row id,
     * {@code NOT NULL}. The same value a
     * {@link app.l2nx.gs.adapter.api.kafka.commands.announcement.DeleteAutoAnnouncementCommand}
     * targets to remove this row.
     */
    public long getId() {
        return id;
    }

    /**
     * Announcement text, already translated to the platform's neutral chat
     * micro-format (literal {@code \n} line breaks, bare URLs). {@code NOT NULL}.
     */
    public String getContent() {
        return content;
    }

    /**
     * Whether the host broadcasts this announcement on the critical/alert
     * channel rather than the normal one. {@code null} when the host schema
     * does not surface a per-row channel flag.
     */
    public @Nullable Boolean getCritical() {
        return critical;
    }

    /**
     * Delay in milliseconds from server start before the first broadcast.
     * {@code null} when not surfaced by the host schema.
     */
    public @Nullable Long getInitialMs() {
        return initialMs;
    }

    /**
     * Repeat period in milliseconds between broadcasts. {@code null} when
     * not surfaced by the host schema.
     */
    public @Nullable Long getDelayMs() {
        return delayMs;
    }

    /**
     * Host-native repeat-count value (semantics — including any "infinite"
     * sentinel — are host-defined). {@code null} when not surfaced by the
     * host schema.
     */
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
