package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one player character. Only {@code id} and {@code name} are required; every other field is
 * null when the tenant does not surface it, including zero sentinels (no clan, not pending deletion).
 * Tick-frequency state (hp/mp/position/lastAccess) is absent: hashing it would storm UPDATEs each cycle.
 */
public final class CharacterDbDto {

    private final long id;
    private final String name;
    private final @Nullable String accountName;
    private final @Nullable String title;
    private final @Nullable Integer level;
    private final @Nullable CharacterDisplayDbDto display;
    private final @Nullable CharacterClass classId;
    private final @Nullable CharacterClass baseClassId;
    private final @Nullable List<CharacterClassDbDto> classes;
    private final @Nullable Long clanId;
    private final @Nullable Integer pvpCounter;
    private final @Nullable Integer pkCounter;
    private final @Nullable Integer karma;
    private final @Nullable Boolean noblesse;
    private final @Nullable Instant scheduledDeletionAt;
    private final @Nullable Boolean online;
    private final @Nullable Long onlineTimeSeconds;
    private final @Nullable Boolean hero;
    private final @Nullable Boolean expBlocked;
    private final @Nullable Integer gearScore;
    private final @Nullable Long fame;
    private final @Nullable String accessLevel;
    private final @Nullable List<CharacterInstanceCooldownDbDto> instanceCooldowns;
    private final @Nullable List<CharacterLockDbDto> locks;

    public CharacterDbDto(
            long id,
            String name,
            @Nullable String accountName,
            @Nullable String title,
            @Nullable Integer level,
            @Nullable CharacterDisplayDbDto display,
            @Nullable CharacterClass classId,
            @Nullable CharacterClass baseClassId,
            @Nullable List<CharacterClassDbDto> classes,
            @Nullable Long clanId,
            @Nullable Integer pvpCounter,
            @Nullable Integer pkCounter,
            @Nullable Integer karma,
            @Nullable Boolean noblesse,
            @Nullable Instant scheduledDeletionAt,
            @Nullable Boolean online,
            @Nullable Long onlineTimeSeconds,
            @Nullable Boolean hero,
            @Nullable Boolean expBlocked,
            @Nullable Integer gearScore,
            @Nullable Long fame,
            @Nullable String accessLevel,
            @Nullable List<CharacterInstanceCooldownDbDto> instanceCooldowns,
            @Nullable List<CharacterLockDbDto> locks) {
        this.id = id;
        this.name = Objects.requireNonNull(name, "CharacterDbDto.name is required");
        this.accountName = accountName;
        this.title = title;
        this.level = level;
        this.display = display;
        this.classId = classId;
        this.baseClassId = baseClassId;
        this.classes = classes == null ? null : Collections.unmodifiableList(classes);
        this.clanId = clanId;
        this.pvpCounter = pvpCounter;
        this.pkCounter = pkCounter;
        this.karma = karma;
        this.noblesse = noblesse;
        this.scheduledDeletionAt = scheduledDeletionAt;
        this.online = online;
        this.onlineTimeSeconds = onlineTimeSeconds;
        this.hero = hero;
        this.expBlocked = expBlocked;
        this.gearScore = gearScore;
        this.fame = fame;
        this.accessLevel = accessLevel;
        this.instanceCooldowns = instanceCooldowns == null ? null : Collections.unmodifiableList(instanceCooldowns);
        this.locks = locks == null ? null : Collections.unmodifiableList(locks);
    }

    public long getId() {
        return id;
    }

    /** Schema providers must drop rows without a name rather than ship placeholders. */
    public String getName() {
        return name;
    }

    public @Nullable String getAccountName() {
        return accountName;
    }

    public @Nullable String getTitle() {
        return title;
    }

    /** Level of the active class. */
    public @Nullable Integer getLevel() {
        return level;
    }

    /** Null when the provider does not sync appearance; the consumer then leaves the stored appearance as is. */
    public @Nullable CharacterDisplayDbDto getDisplay() {
        return display;
    }

    /** Null when the source id is not in the canonical {@link CharacterClass} set. */
    public @Nullable CharacterClass getClassId() {
        return classId;
    }

    /** Equals {@link #getClassId()} when no subclass slot was used. */
    public @Nullable CharacterClass getBaseClassId() {
        return baseClassId;
    }

    /**
     * Full class roster: one MAIN entry plus one SUB per subclass, in provider order.
     * Null when the tenant does not sync classes; empty when none resolved to a canonical class.
     */
    public @Nullable List<CharacterClassDbDto> getClasses() {
        return classes;
    }

    /** Null for the "no clan" sentinel ({@code clanid = 0}). */
    public @Nullable Long getClanId() {
        return clanId;
    }

    public @Nullable Integer getPvpCounter() {
        return pvpCounter;
    }

    public @Nullable Integer getPkCounter() {
        return pkCounter;
    }

    public @Nullable Integer getKarma() {
        return karma;
    }

    public @Nullable Boolean getNoblesse() {
        return noblesse;
    }

    /** Null for the "not pending deletion" sentinel ({@code deletetime = 0}). */
    public @Nullable Instant getScheduledDeletionAt() {
        return scheduledDeletionAt;
    }

    /** Coarse (~60s) CDC backstop; the platform reconciles it with the runtime channel. */
    public @Nullable Boolean getOnline() {
        return online;
    }

    /** Stale by up to one autosave interval for an online character. */
    public @Nullable Long getOnlineTimeSeconds() {
        return onlineTimeSeconds;
    }

    /** True for a hero in the active Olympiad cycle; historical crownings are carried by {@code HeroGrantedEvent}. */
    public @Nullable Boolean getHero() {
        return hero;
    }

    public @Nullable Boolean getExpBlocked() {
        return expBlocked;
    }

    /** Snapshot of the active class at the last character store, not live. */
    public @Nullable Integer getGearScore() {
        return gearScore;
    }

    public @Nullable Long getFame() {
        return fame;
    }

    /** Opaque: numeric text on int-based builds, role name on string-role builds. */
    public @Nullable String getAccessLevel() {
        return accessLevel;
    }

    /** Null when the tenant does not sync cooldowns; empty when the character has none. */
    public @Nullable List<CharacterInstanceCooldownDbDto> getInstanceCooldowns() {
        return instanceCooldowns;
    }

    /** Each entry's {@code lockType} is a {@link WellKnownCharacterLockTypes} value. Null when the tenant does not sync locks. */
    public @Nullable List<CharacterLockDbDto> getLocks() {
        return locks;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .name(name)
                .accountName(accountName)
                .title(title)
                .level(level)
                .display(display)
                .classId(classId)
                .baseClassId(baseClassId)
                .classes(classes)
                .clanId(clanId)
                .pvpCounter(pvpCounter)
                .pkCounter(pkCounter)
                .karma(karma)
                .noblesse(noblesse)
                .scheduledDeletionAt(scheduledDeletionAt)
                .online(online)
                .onlineTimeSeconds(onlineTimeSeconds)
                .hero(hero)
                .expBlocked(expBlocked)
                .gearScore(gearScore)
                .fame(fame)
                .accessLevel(accessLevel)
                .instanceCooldowns(instanceCooldowns)
                .locks(locks);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterDbDto)) return false;
        CharacterDbDto that = (CharacterDbDto) o;
        return id == that.id
                && name.equals(that.name)
                && Objects.equals(accountName, that.accountName)
                && Objects.equals(title, that.title)
                && Objects.equals(level, that.level)
                && Objects.equals(display, that.display)
                && classId == that.classId
                && baseClassId == that.baseClassId
                && Objects.equals(classes, that.classes)
                && Objects.equals(clanId, that.clanId)
                && Objects.equals(pvpCounter, that.pvpCounter)
                && Objects.equals(pkCounter, that.pkCounter)
                && Objects.equals(karma, that.karma)
                && Objects.equals(noblesse, that.noblesse)
                && Objects.equals(scheduledDeletionAt, that.scheduledDeletionAt)
                && Objects.equals(online, that.online)
                && Objects.equals(onlineTimeSeconds, that.onlineTimeSeconds)
                && Objects.equals(hero, that.hero)
                && Objects.equals(expBlocked, that.expBlocked)
                && Objects.equals(gearScore, that.gearScore)
                && Objects.equals(fame, that.fame)
                && Objects.equals(accessLevel, that.accessLevel)
                && Objects.equals(instanceCooldowns, that.instanceCooldowns)
                && Objects.equals(locks, that.locks);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                id,
                name,
                accountName,
                title,
                level,
                display,
                classId,
                baseClassId,
                classes,
                clanId,
                pvpCounter,
                pkCounter,
                karma,
                noblesse,
                scheduledDeletionAt,
                online,
                onlineTimeSeconds,
                hero,
                expBlocked,
                gearScore,
                fame,
                accessLevel,
                instanceCooldowns,
                locks);
    }

    @Override
    public String toString() {
        return "CharacterDbDto[id=" + id
                + ", name=" + name
                + ", accountName=" + accountName
                + ", title=" + title
                + ", level=" + level
                + ", display=" + display
                + ", classId=" + classId
                + ", baseClassId=" + baseClassId
                + ", classes=" + classes
                + ", clanId=" + clanId
                + ", pvpCounter=" + pvpCounter
                + ", pkCounter=" + pkCounter
                + ", karma=" + karma
                + ", noblesse=" + noblesse
                + ", scheduledDeletionAt=" + scheduledDeletionAt
                + ", online=" + online
                + ", onlineTimeSeconds=" + onlineTimeSeconds
                + ", hero=" + hero
                + ", expBlocked=" + expBlocked
                + ", gearScore=" + gearScore
                + ", fame=" + fame
                + ", accessLevel=" + accessLevel
                + ", instanceCooldowns=" + instanceCooldowns
                + ", locks=" + locks + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable String name;
        private @Nullable String accountName;
        private @Nullable String title;
        private @Nullable Integer level;
        private @Nullable CharacterDisplayDbDto display;
        private @Nullable CharacterClass classId;
        private @Nullable CharacterClass baseClassId;
        private @Nullable List<CharacterClassDbDto> classes;
        private @Nullable Long clanId;
        private @Nullable Integer pvpCounter;
        private @Nullable Integer pkCounter;
        private @Nullable Integer karma;
        private @Nullable Boolean noblesse;
        private @Nullable Instant scheduledDeletionAt;
        private @Nullable Boolean online;
        private @Nullable Long onlineTimeSeconds;
        private @Nullable Boolean hero;
        private @Nullable Boolean expBlocked;
        private @Nullable Integer gearScore;
        private @Nullable Long fame;
        private @Nullable String accessLevel;
        private @Nullable List<CharacterInstanceCooldownDbDto> instanceCooldowns;
        private @Nullable List<CharacterLockDbDto> locks;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder accountName(@Nullable String accountName) {
            this.accountName = accountName;
            return this;
        }

        public Builder title(@Nullable String title) {
            this.title = title;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
            return this;
        }

        public Builder display(@Nullable CharacterDisplayDbDto display) {
            this.display = display;
            return this;
        }

        public Builder classId(@Nullable CharacterClass classId) {
            this.classId = classId;
            return this;
        }

        public Builder baseClassId(@Nullable CharacterClass baseClassId) {
            this.baseClassId = baseClassId;
            return this;
        }

        public Builder classes(@Nullable List<CharacterClassDbDto> classes) {
            this.classes = classes;
            return this;
        }

        public Builder clanId(@Nullable Long clanId) {
            this.clanId = clanId;
            return this;
        }

        public Builder pvpCounter(@Nullable Integer pvpCounter) {
            this.pvpCounter = pvpCounter;
            return this;
        }

        public Builder pkCounter(@Nullable Integer pkCounter) {
            this.pkCounter = pkCounter;
            return this;
        }

        public Builder karma(@Nullable Integer karma) {
            this.karma = karma;
            return this;
        }

        public Builder noblesse(@Nullable Boolean noblesse) {
            this.noblesse = noblesse;
            return this;
        }

        public Builder scheduledDeletionAt(@Nullable Instant scheduledDeletionAt) {
            this.scheduledDeletionAt = scheduledDeletionAt;
            return this;
        }

        public Builder online(@Nullable Boolean online) {
            this.online = online;
            return this;
        }

        public Builder onlineTimeSeconds(@Nullable Long onlineTimeSeconds) {
            this.onlineTimeSeconds = onlineTimeSeconds;
            return this;
        }

        public Builder hero(@Nullable Boolean hero) {
            this.hero = hero;
            return this;
        }

        public Builder expBlocked(@Nullable Boolean expBlocked) {
            this.expBlocked = expBlocked;
            return this;
        }

        public Builder gearScore(@Nullable Integer gearScore) {
            this.gearScore = gearScore;
            return this;
        }

        public Builder fame(@Nullable Long fame) {
            this.fame = fame;
            return this;
        }

        public Builder accessLevel(@Nullable String accessLevel) {
            this.accessLevel = accessLevel;
            return this;
        }

        public Builder instanceCooldowns(@Nullable List<CharacterInstanceCooldownDbDto> instanceCooldowns) {
            this.instanceCooldowns = instanceCooldowns;
            return this;
        }

        public Builder locks(@Nullable List<CharacterLockDbDto> locks) {
            this.locks = locks;
            return this;
        }

        public CharacterDbDto build() {
            return new CharacterDbDto(
                    id,
                    name,
                    accountName,
                    title,
                    level,
                    display,
                    classId,
                    baseClassId,
                    classes,
                    clanId,
                    pvpCounter,
                    pkCounter,
                    karma,
                    noblesse,
                    scheduledDeletionAt,
                    online,
                    onlineTimeSeconds,
                    hero,
                    expBlocked,
                    gearScore,
                    fame,
                    accessLevel,
                    instanceCooldowns,
                    locks);
        }
    }
}
