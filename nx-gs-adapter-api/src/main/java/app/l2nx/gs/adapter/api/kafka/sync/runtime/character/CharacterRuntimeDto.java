package app.l2nx.gs.adapter.api.kafka.sync.runtime.character;

import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wire DTO for one player character's volatile runtime state — payload of
 * {@code SyncEvent<CharacterRuntimeDto>} on the platform-supplied per-tenant
 * runtime character sync topic
 * ({@code <tenant>.gs.sync.runtime.character}).
 *
 * <p>Sibling of {@code app.l2nx.gs.adapter.api.kafka.sync.db.character.CharacterDbDto}
 * (DB-derived persistent character state). Both DTOs share {@code id} (source-side
 * {@code charId} / {@code objectId}) so platform consumers can join the two streams
 * by primary key.</p>
 *
 * <p>Only {@link #getId() id} is required; everything else is optional. Different
 * tenants populate different subsets — e.g. cores without a vitality mechanic
 * leave {@code curVit}/{@code maxVit} null. Null fields are omitted from the
 * Gson wire when {@code serializeNulls=false} on the platform-side producer.</p>
 *
 * <p>Presence ({@link #getOnline() online}) drives platform-side reconciliation
 * of the per-character "is this player currently logged in" signal. Wire
 * convention picked for byte-budget at high-load tick rates:
 * <ul>
 *   <li>Regular live-state row: {@code online} left {@code null} — the producer
 *   omits it from the JSON. Platform consumers MUST treat omitted /
 *   {@code null} as {@code online=true} (the historical wire emitted runtime
 *   rows only for online characters).</li>
 *   <li>One-shot offline tombstone: {@code online=false} explicit, vitals /
 *   coordinates all {@code null} (also omitted from the JSON). The row exists
 *   only to flip platform-side presence to "offline".</li>
 *   <li>{@code online=true} explicit is permitted but redundant — producers
 *   should prefer {@code null} for the regular ONLINE case to save bytes.</li>
 * </ul>
 * Why a runtime-channel signal and not a CDC column on {@code CharacterDbDto}:
 * login / logout would inflate CDC UPDATE volume per cycle, and CDC tick
 * cadence is too coarse to surface presence reactively.</p>
 *
 * <p>Activity signals ({@link #getAiStatus() aiStatus},
 * {@link #getCustomActivities() customActivities}) describe "what the character
 * is doing" for dashboard / presence consumers. They are <b>independent</b> —
 * no precedence between them is implied by the wire:
 * <ul>
 *   <li>{@code aiStatus} — engine-native control intention (canonical values in
 *   {@link WellKnownAiStatuses}). Transient: flips with movement / combat much
 *   like {@code x}/{@code y}/{@code z}.</li>
 *   <li>{@code customActivities} — build-specific sustained activities as a
 *   <b>list</b> of structured {@link CustomActivity} entries ({@code type} +
 *   open {@code metadata}; e.g. fishing with elapsed-time / penalty-tier
 *   metadata, or autofarming with remaining-time metadata). A character can be
 *   in several at once (e.g. autofarming while fishing), so the wire carries
 *   them as a JSON array. Long-lived; {@code null} / omitted when the character
 *   is not in any special activity. The activity set varies per core, so each
 *   entry is open: hosts emit their own type / metadata keys and consumers
 *   tolerate unknown values.</li>
 * </ul>
 * Both are {@code null} on offline tombstones and on hosts that do not populate
 * them.</p>
 */
public final class CharacterRuntimeDto {

    private final long id;
    private final @Nullable Integer curHp;
    private final @Nullable Integer maxHp;
    private final @Nullable Integer curMp;
    private final @Nullable Integer maxMp;
    private final @Nullable Integer curCp;
    private final @Nullable Integer maxCp;
    private final @Nullable Integer curVit;
    private final @Nullable Integer maxVit;
    private final @Nullable Integer x;
    private final @Nullable Integer y;
    private final @Nullable Integer z;
    private final @Nullable Boolean online;
    private final @Nullable String aiStatus;
    private final @Nullable List<CustomActivity> customActivities;

    /**
     * Canonical constructor. Prefer {@link #builder()} — positional construction
     * of 15 mostly-nullable fields is error-prone.
     */
    public CharacterRuntimeDto(long id,
                               @Nullable Integer curHp,
                               @Nullable Integer maxHp,
                               @Nullable Integer curMp,
                               @Nullable Integer maxMp,
                               @Nullable Integer curCp,
                               @Nullable Integer maxCp,
                               @Nullable Integer curVit,
                               @Nullable Integer maxVit,
                               @Nullable Integer x,
                               @Nullable Integer y,
                               @Nullable Integer z,
                               @Nullable Boolean online,
                               @Nullable String aiStatus,
                               @Nullable List<CustomActivity> customActivities) {
        this.id = id;
        this.curHp = curHp;
        this.maxHp = maxHp;
        this.curMp = curMp;
        this.maxMp = maxMp;
        this.curCp = curCp;
        this.maxCp = maxCp;
        this.curVit = curVit;
        this.maxVit = maxVit;
        this.x = x;
        this.y = y;
        this.z = z;
        this.online = online;
        this.aiStatus = aiStatus;
        this.customActivities = customActivities == null
                ? null
                : Collections.unmodifiableList(new ArrayList<CustomActivity>(customActivities));
    }

    /**
     * Primary key — source {@code charId} / {@code objectId}, {@code NOT NULL}.
     * Same value as {@code CharacterDbDto.id} for platform-side join.
     */
    public long getId() {
        return id;
    }

    public @Nullable Integer getCurHp() {
        return curHp;
    }

    public @Nullable Integer getMaxHp() {
        return maxHp;
    }

    public @Nullable Integer getCurMp() {
        return curMp;
    }

    public @Nullable Integer getMaxMp() {
        return maxMp;
    }

    public @Nullable Integer getCurCp() {
        return curCp;
    }

    public @Nullable Integer getMaxCp() {
        return maxCp;
    }

    /**
     * Current vitality (stamina) — L2-specific mechanic. {@code null} on cores
     * without vitality.
     */
    public @Nullable Integer getCurVit() {
        return curVit;
    }

    public @Nullable Integer getMaxVit() {
        return maxVit;
    }

    public @Nullable Integer getX() {
        return x;
    }

    public @Nullable Integer getY() {
        return y;
    }

    public @Nullable Integer getZ() {
        return z;
    }

    /**
     * Presence marker. {@code null} or {@code true} on regular live-state rows
     * (producer convention: omit from the wire for byte-budget). Explicit
     * {@code false} on one-shot offline tombstones — vitals / coordinates are
     * typically {@code null} on those. Platform consumers MUST treat omitted /
     * {@code null} as {@code online=true} for back-compat with legacy
     * providers and the byte-optimized regular path.
     */
    public @Nullable Boolean getOnline() {
        return online;
    }

    /**
     * Engine-native AI control intention — the reactive server-side state the
     * core puts the character in (idle / moving / attack / cast / …). Open
     * string; canonical lower_snake_case values in {@link WellKnownAiStatuses}.
     * {@code null} when the host does not report it or on offline tombstones.
     * Transient by nature — flips with movement / combat. Independent of
     * {@link #getCustomActivities() customActivities}.
     */
    public @Nullable String getAiStatus() {
        return aiStatus;
    }

    /**
     * Build-specific sustained activities — the high-level "what the player is
     * occupied with" signals that live outside the engine AI state machine
     * (e.g. fishing, reading a book, autofarming). A <b>list</b> of structured
     * {@link CustomActivity} entries because a character can be in several at
     * once (e.g. autofarming while fishing). Each entry carries a required
     * {@code type} discriminator (canonical values in
     * {@link WellKnownCustomActivities}) plus an open {@code metadata} map for
     * activity-specific extras (canonical keys in
     * {@link WellKnownCustomActivityMetadata}). The set varies per core, so the
     * envelope stays agnostic — hosts emit their own type / keys and consumers
     * tolerate unknowns. {@code null} when the character is in no special
     * activity, the host does not report it, or on offline tombstones; when
     * non-null the returned list is unmodifiable. Independent of
     * {@link #getAiStatus() aiStatus} — no precedence between the two.
     */
    public @Nullable List<CustomActivity> getCustomActivities() {
        return customActivities;
    }

    /**
     * Resolves the {@link #getOnline() online} wire field to a primitive
     * presence value per the wire convention: omitted / {@code null} /
     * {@code true} all mean ONLINE; only an explicit {@code false} (one-shot
     * offline tombstone) means OFFLINE. Single point of truth for consumers
     * — keeps the {@code null = ONLINE} byte-budget rule from leaking into
     * every call site.
     */
    public boolean isOnlineEffective() {
        return online == null || online;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .curHp(curHp)
                .maxHp(maxHp)
                .curMp(curMp)
                .maxMp(maxMp)
                .curCp(curCp)
                .maxCp(maxCp)
                .curVit(curVit)
                .maxVit(maxVit)
                .x(x)
                .y(y)
                .z(z)
                .online(online)
                .aiStatus(aiStatus)
                .customActivities(customActivities);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterRuntimeDto)) return false;
        CharacterRuntimeDto that = (CharacterRuntimeDto) o;
        return id == that.id
                && Objects.equals(curHp, that.curHp)
                && Objects.equals(maxHp, that.maxHp)
                && Objects.equals(curMp, that.curMp)
                && Objects.equals(maxMp, that.maxMp)
                && Objects.equals(curCp, that.curCp)
                && Objects.equals(maxCp, that.maxCp)
                && Objects.equals(curVit, that.curVit)
                && Objects.equals(maxVit, that.maxVit)
                && Objects.equals(x, that.x)
                && Objects.equals(y, that.y)
                && Objects.equals(z, that.z)
                && Objects.equals(online, that.online)
                && Objects.equals(aiStatus, that.aiStatus)
                && Objects.equals(customActivities, that.customActivities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, curHp, maxHp, curMp, maxMp, curCp, maxCp,
                curVit, maxVit, x, y, z, online, aiStatus, customActivities);
    }

    @Override
    public String toString() {
        return "CharacterRuntimeDto[id=" + id
                + ", curHp=" + curHp + ", maxHp=" + maxHp
                + ", curMp=" + curMp + ", maxMp=" + maxMp
                + ", curCp=" + curCp + ", maxCp=" + maxCp
                + ", curVit=" + curVit + ", maxVit=" + maxVit
                + ", x=" + x + ", y=" + y + ", z=" + z
                + ", online=" + online
                + ", aiStatus=" + aiStatus
                + ", customActivities=" + customActivities + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable Integer curHp;
        private @Nullable Integer maxHp;
        private @Nullable Integer curMp;
        private @Nullable Integer maxMp;
        private @Nullable Integer curCp;
        private @Nullable Integer maxCp;
        private @Nullable Integer curVit;
        private @Nullable Integer maxVit;
        private @Nullable Integer x;
        private @Nullable Integer y;
        private @Nullable Integer z;
        private @Nullable Boolean online;
        private @Nullable String aiStatus;
        private @Nullable List<CustomActivity> customActivities;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder curHp(@Nullable Integer curHp) {
            this.curHp = curHp;
            return this;
        }

        public Builder maxHp(@Nullable Integer maxHp) {
            this.maxHp = maxHp;
            return this;
        }

        public Builder curMp(@Nullable Integer curMp) {
            this.curMp = curMp;
            return this;
        }

        public Builder maxMp(@Nullable Integer maxMp) {
            this.maxMp = maxMp;
            return this;
        }

        public Builder curCp(@Nullable Integer curCp) {
            this.curCp = curCp;
            return this;
        }

        public Builder maxCp(@Nullable Integer maxCp) {
            this.maxCp = maxCp;
            return this;
        }

        public Builder curVit(@Nullable Integer curVit) {
            this.curVit = curVit;
            return this;
        }

        public Builder maxVit(@Nullable Integer maxVit) {
            this.maxVit = maxVit;
            return this;
        }

        public Builder x(@Nullable Integer x) {
            this.x = x;
            return this;
        }

        public Builder y(@Nullable Integer y) {
            this.y = y;
            return this;
        }

        public Builder z(@Nullable Integer z) {
            this.z = z;
            return this;
        }

        public Builder online(@Nullable Boolean online) {
            this.online = online;
            return this;
        }

        /**
         * Engine-native AI control intention — canonical values in
         * {@link WellKnownAiStatuses}. Open string; {@code null} when not reported.
         */
        public Builder aiStatus(@Nullable String aiStatus) {
            this.aiStatus = aiStatus;
            return this;
        }

        /**
         * Build-specific sustained activities — a list of structured
         * {@link CustomActivity} entries ({@code type} + open {@code metadata}).
         * {@code null} when the character is in no special activity. Defensively
         * copied on {@link #build()}.
         */
        public Builder customActivities(@Nullable List<CustomActivity> customActivities) {
            this.customActivities = customActivities;
            return this;
        }

        public CharacterRuntimeDto build() {
            return new CharacterRuntimeDto(id, curHp, maxHp, curMp, maxMp,
                    curCp, maxCp, curVit, maxVit, x, y, z, online,
                    aiStatus, customActivities);
        }
    }
}
