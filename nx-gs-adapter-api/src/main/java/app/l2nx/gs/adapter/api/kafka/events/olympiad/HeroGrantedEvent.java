package app.l2nx.gs.adapter.api.kafka.events.olympiad;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.util.*;
import org.jspecify.annotations.Nullable;

/**
 * Character crowned hero at the end of an Olympiad cycle; one event per hero, on the {@code olympiad} family topic.
 * <p>Partitioned by {@link #getCharId() charId} (8-byte BE) like {@link OlympiadMatchResultEvent}, so a character's matches and crownings stay ordered.
 * <p>{@link #getEventId() eventId} MUST be a UUIDv7 (upper 48 bits encode the timestamp); consumers dedupe on it (at-least-once).
 * <p>Names are not carried - join on charId / clanId via CDC. Current hero state is CDC {@code CharacterDbDto.hero}; this event is the historical record.
 * <p>{@link #getMetadata() metadata} is an optional open string-to-string map; {@code null} when absent, consumers ignore unknown keys.
 */
public final class HeroGrantedEvent {

    private final UUID eventId;
    private final long charId;
    private final int classId;
    private final @Nullable CharacterClass clazz;
    private final @Nullable Long clanId;
    private final int olympiadCycle;
    private final @Nullable Map<String, String> metadata;

    public HeroGrantedEvent(
            UUID eventId,
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
        this.metadata =
                metadata == null ? null : Collections.unmodifiableMap(new LinkedHashMap<String, String>(metadata));
    }

    public UUID getEventId() {
        return eventId;
    }

    public long getCharId() {
        return charId;
    }

    /** Legacy host-numbered class id; consumers MUST prefer {@link #getClazz() clazz} when non-null. */
    public int getClassId() {
        return classId;
    }

    /** {@code null} from hosts not yet migrated (fall back to {@link #getClassId() classId}) or when the class is outside the canonical {@link CharacterClass} set. */
    public @Nullable CharacterClass getClazz() {
        return clazz;
    }

    /** {@code null} when the character has no clan or the host could not resolve it (e.g. offline winner). */
    public @Nullable Long getClanId() {
        return clanId;
    }

    public int getOlympiadCycle() {
        return olympiadCycle;
    }

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
