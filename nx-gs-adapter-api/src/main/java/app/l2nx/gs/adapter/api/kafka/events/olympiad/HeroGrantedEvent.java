package app.l2nx.gs.adapter.api.kafka.events.olympiad;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import org.jspecify.annotations.Nullable;

import java.util.*;

/**
 * Wire DTO published to the {@code olympiad} family topic
 * ({@code <tenant>.gs.events.olympiad}) when a character is crowned hero at
 * the end of an Olympiad cycle. One event per crowned hero.
 *
 * <p>Rides the same family as {@link OlympiadMatchResultEvent} — a multi-event
 * family dispatched on the {@code Nx-Message-Type} header — and shares its
 * partition key, {@link #getCharId() charId} (8-byte big-endian), so a
 * character's match history and hero crownings land on one partition in
 * occurrence order.</p>
 *
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7. The crowning timestamp is
 * encoded in the upper 48 bits — extractable via
 * {@code app.l2nx.gs.commons.UUIDv7.extractCreatedAt(eventId)}; no separate
 * {@code crownedAt} field. Platform consumers dedupe on {@code eventId}
 * (at-least-once delivery).</p>
 *
 * <p>Character / clan names are intentionally NOT carried — the platform joins
 * on {@link #getCharId() charId} / {@link #getClanId() clanId} against the
 * character / clan CDC streams. Current "is this character a hero right now"
 * lives on the CDC {@code CharacterDbDto.hero} flag; this event is the durable
 * historical record of each crowning.</p>
 *
 * <p>{@link #getMetadata() metadata} — optional open string→string map of
 * build-agnostic attributes about this crowning. {@code null} when absent;
 * hosts MAY add arbitrary keys without an API release, and consumers ignore
 * keys they do not understand.</p>
 *
 * <p>Java-8 POJO; {@code -parameters} javac flag preserves constructor
 * parameter names so Gson / Jackson can deserialize without
 * {@code @JsonProperty}.</p>
 */
public final class HeroGrantedEvent {

    private final UUID eventId;
    private final long charId;
    private final int classId;
    private final @Nullable CharacterClass clazz;
    private final @Nullable Long clanId;
    private final int olympiadCycle;
    private final @Nullable Map<String, String> metadata;

    public HeroGrantedEvent(UUID eventId,
                            long charId,
                            int classId,
                            @Nullable CharacterClass clazz,
                            @Nullable Long clanId,
                            int olympiadCycle,
                            @Nullable Map<String, String> metadata) {
        this.eventId = Objects.requireNonNull(eventId, "HeroGrantedEvent.eventId is required");
        this.charId = charId;
        this.classId = classId;
        this.clazz = clazz;
        this.clanId = clanId;
        this.olympiadCycle = olympiadCycle;
        this.metadata = metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    /**
     * Event identity. MUST be a UUIDv7 — the upper 48 bits encode the crowning
     * timestamp.
     */
    public UUID getEventId() {
        return eventId;
    }

    /**
     * Crowned character's {@code objectId} — partition key (8-byte big-endian),
     * shared with {@link OlympiadMatchResultEvent}.
     */
    public long getCharId() {
        return charId;
    }

    /**
     * Legacy numeric class id the character was crowned hero with. Source-side
     * (host) numbering. Superseded by {@link #getClazz() clazz}; retained for
     * back-compat while hosts migrate to the canonical token. Consumers MUST
     * prefer {@code clazz} when it is non-null.
     */
    public int getClassId() {
        return classId;
    }

    /**
     * Canonical, source-agnostic class token the character was crowned hero
     * with. {@code null} from hosts that have not yet migrated off the numeric
     * {@link #getClassId() classId} (consumers fall back to it then), or when
     * the source class is not in the canonical {@link CharacterClass} set.
     */
    public @Nullable CharacterClass getClazz() {
        return clazz;
    }

    /**
     * Clan affiliation snapshot at crowning. {@code null} when the character
     * has no clan or the host could not resolve it best-effort (e.g. an offline
     * winner) — platform consumers join on the clan CDC stream when needed.
     */
    public @Nullable Long getClanId() {
        return clanId;
    }

    /**
     * Olympiad cycle this character was crowned hero for.
     */
    public int getOlympiadCycle() {
        return olympiadCycle;
    }

    /**
     * Optional open string→string map of build-agnostic attributes about this
     * crowning. {@code null} when absent; hosts MAY add arbitrary keys without
     * an API release.
     */
    public @Nullable Map<String, String> getMetadata() {
        return metadata;
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId)
                .charId(charId)
                .classId(classId)
                .clazz(clazz)
                .clanId(clanId)
                .olympiadCycle(olympiadCycle)
                .metadata(metadata);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof HeroGrantedEvent)) return false;
        HeroGrantedEvent that = (HeroGrantedEvent) o;
        return charId == that.charId
                && classId == that.classId
                && olympiadCycle == that.olympiadCycle
                && eventId.equals(that.eventId)
                && clazz == that.clazz
                && Objects.equals(clanId, that.clanId)
                && Objects.equals(metadata, that.metadata);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, charId, classId, clazz, clanId, olympiadCycle, metadata);
    }

    @Override
    public String toString() {
        return "HeroGrantedEvent[eventId=" + eventId
                + ", charId=" + charId
                + ", classId=" + classId
                + ", clazz=" + clazz
                + ", clanId=" + clanId
                + ", olympiadCycle=" + olympiadCycle
                + ", metadata=" + metadata + "]";
    }

    public static final class Builder {
        private @Nullable UUID eventId;
        private long charId;
        private int classId;
        private @Nullable CharacterClass clazz;
        private @Nullable Long clanId;
        private int olympiadCycle;
        private @Nullable Map<String, String> metadata;

        public Builder eventId(UUID eventId) {
            this.eventId = eventId;
            return this;
        }

        public Builder charId(long charId) {
            this.charId = charId;
            return this;
        }

        public Builder classId(int classId) {
            this.classId = classId;
            return this;
        }

        public Builder clazz(@Nullable CharacterClass clazz) {
            this.clazz = clazz;
            return this;
        }

        public Builder clanId(@Nullable Long clanId) {
            this.clanId = clanId;
            return this;
        }

        public Builder olympiadCycle(int olympiadCycle) {
            this.olympiadCycle = olympiadCycle;
            return this;
        }

        public Builder metadata(@Nullable Map<String, String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public HeroGrantedEvent build() {
            return new HeroGrantedEvent(eventId, charId, classId, clazz, clanId, olympiadCycle, metadata);
        }
    }
}
