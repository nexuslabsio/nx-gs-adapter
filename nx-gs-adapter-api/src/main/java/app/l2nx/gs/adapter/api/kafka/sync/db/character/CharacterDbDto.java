package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.CharacterSex;
import app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClass;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one player character, payload of
 * {@code SyncEvent<CharacterDbDto>} on the platform-supplied per-tenant
 * character sync topic (e.g. {@code bohpts.gs.sync.characters}).
 *
 * <p>Required fields: {@link #getId() id} (source-side {@code charId}) and
 * {@link #getName() name} (source-side {@code char_name}). Both are
 * structurally guaranteed by every L2J-derived schema (PK + NOT NULL on
 * {@code char_name}); a row without either is dirty data that the schema
 * provider MUST drop with a WARN before constructing the DTO. The builder
 * enforces non-null name; null at construction time throws
 * {@link NullPointerException} — fail-loud on dirty assembly rather than
 * shipping placeholder data downstream.</p>
 *
 * <p>Everything else is optional. Different tenants populate different
 * subsets depending on which columns exist in their schema and which the
 * tenant chose to surface — schema providers control this via
 * {@code PrimarySource.hashedColumns()} and what they put into the row in
 * {@code mapRow()}.</p>
 *
 * <p>Sentinel mapping: most game-server schemas use {@code 0} as the
 * "no clan" sentinel in {@code characters.clanid} and as the "not pending
 * deletion" sentinel in {@code characters.deletetime}. Schema providers
 * translate sentinel-zero (and source SQL NULL) to {@code null} when
 * populating {@code clanId} / {@code scheduledDeletionAt}; platform consumers see
 * explicit nulls.</p>
 *
 * <p>The persisted {@code online} flag is surfaced as a coarse CDC backstop
 * (see {@link #getOnline()}); authoritative real-time presence lives on the
 * sibling runtime channel ({@code CharacterRuntimeDto.online}) and discrete
 * {@code CharacterPresenceEvent}s, reconciled by platform-side consumers.</p>
 *
 * <p>Tick-frequency volatile state ({@code curHp}/{@code curMp}/{@code x}/
 * {@code y}/{@code z}/{@code lastAccess} and similar) is
 * intentionally not modeled — including such fields in a poll-based CDC hash
 * would cause an UPDATE storm for every online character on every cycle;
 * real-time state belongs on a separate event channel. Accumulated
 * {@link #getOnlineTimeSeconds() online time} is the exception: the source
 * column advances only when the row is persisted (logout + periodic
 * autosave), not at tick frequency, so it is CDC-tolerable.</p>
 *
 * <p>Experience and SP are a second, narrower exception: they ARE surfaced —
 * per class, inside {@link #getClasses()} — but only as unhashed ride-alongs,
 * so they never trigger a sync event and are never fresher than the source's
 * last full store. Live figures for the class being played ride the runtime
 * channel instead.</p>
 */
public final class CharacterDbDto {

    private final long id;
    private final String name;
    private final @Nullable String accountName;
    private final @Nullable String title;
    private final @Nullable Integer level;
    private final @Nullable CharacterSex sex;
    private final @Nullable CharacterRace race;
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
            @Nullable CharacterSex sex,
            @Nullable CharacterRace race,
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
        this.sex = sex;
        this.race = race;
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

    /**
     * Primary key — source {@code charId}, {@code NOT NULL}.
     */
    public long getId() {
        return id;
    }

    /**
     * Character name — source {@code char_name}, {@code NOT NULL}. Schema
     * providers MUST skip rows where the source column is null/missing
     * rather than shipping placeholders.
     */
    public String getName() {
        return name;
    }

    /**
     * Login account owning this character — source {@code account_name}.
     * {@code null} when the tenant does not surface this column. Used by
     * platform consumers as a generic per-character account label and as a
     * filter dimension; the field is generic across L2 forks (vanilla L2J,
     * Lucera, Essence all carry it on {@code characters}).
     */
    public @Nullable String getAccountName() {
        return accountName;
    }

    /**
     * Display title.
     */
    public @Nullable String getTitle() {
        return title;
    }

    /**
     * Current character level (active class).
     */
    public @Nullable Integer getLevel() {
        return level;
    }

    /**
     * Character sex.
     */
    public @Nullable CharacterSex getSex() {
        return sex;
    }

    /**
     * Character race.
     */
    public @Nullable CharacterRace getRace() {
        return race;
    }

    /**
     * Active class. {@code null} when the source ID is not part of the
     * canonical class set surfaced by {@link CharacterClass}, or when the
     * tenant does not surface this column.
     */
    public @Nullable CharacterClass getClassId() {
        return classId;
    }

    /**
     * Base (root) class — source {@code base_class}. Equal to
     * {@link #getClassId()} for characters that have not used a subclass /
     * dual-class slot.
     */
    public @Nullable CharacterClass getBaseClassId() {
        return baseClassId;
    }

    /**
     * The character's full class roster — exactly one
     * {@link app.l2nx.gs.adapter.api.domain.character.clazz.CharacterClassKind#MAIN}
     * entry plus one {@code SUB} entry per subclass, ordered as the schema
     * provider's {@code mapEntity} produced them (no platform-side ordering
     * contract).
     *
     * <p>Assembled by the schema provider so that fork-specific storage —
     * main class on the character row versus a side table that also holds
     * it — never reaches consumers. Which entry is currently being played
     * is given by {@link #getClassId()}, not by a flag on the entry.</p>
     *
     * <p>{@code null} when the tenant does not sync classes at all; empty
     * list when it syncs them but the character resolved to no canonical
     * class.</p>
     */
    public @Nullable List<CharacterClassDbDto> getClasses() {
        return classes;
    }

    /**
     * Clan membership. {@code null} when the source {@code clanid = 0}
     * (the conventional "no clan" sentinel) or SQL NULL or when the
     * tenant does not surface this column.
     */
    public @Nullable Long getClanId() {
        return clanId;
    }

    /**
     * PvP kill counter.
     */
    public @Nullable Integer getPvpCounter() {
        return pvpCounter;
    }

    /**
     * PK (player-kill) counter.
     */
    public @Nullable Integer getPkCounter() {
        return pkCounter;
    }

    /**
     * Karma score (negative reputation accrued from PKs).
     */
    public @Nullable Integer getKarma() {
        return karma;
    }

    /**
     * Noblesse status — source {@code nobless} (typically tinyint 0/1).
     * {@code null} when the tenant does not surface this column.
     */
    public @Nullable Boolean getNoblesse() {
        return noblesse;
    }

    /**
     * Pending-deletion timestamp — source {@code deletetime} (typically
     * epoch-millis BIGINT, with {@code 0} as the "not pending deletion"
     * sentinel). Schema providers translate sentinel-zero / SQL NULL to
     * {@code null}; non-null values denote when the character will be
     * (or was scheduled to be) hard-deleted by the game server.
     */
    public @Nullable Instant getScheduledDeletionAt() {
        return scheduledDeletionAt;
    }

    /**
     * Persisted online flag from the source row — typically
     * {@code characters.online} (TINYINT 0/1) on L2J schemas.
     * {@code null} when the tenant does not surface this column.
     *
     * <p>One of three sources platform consumers reconcile into the
     * {@code online} column on {@code gs_characters} (the others being
     * the runtime channel's {@code CharacterRuntimeDto.online} and
     * discrete {@code CharacterPresenceEvent}s). Timestamp-based
     * last-writer-wins on the platform side — CDC source has the
     * coarsest tick cadence (~60s) and acts as the authoritative
     * backstop after adapter restart, when the runtime channel's
     * in-memory previous-online set is empty.</p>
     */
    public @Nullable Boolean getOnline() {
        return online;
    }

    /**
     * Accumulated total online time in seconds — source {@code onlinetime}.
     * The game server rewrites this column to the live total (stored baseline
     * + current-session elapsed) on every full store (logout + periodic
     * autosave), so it advances at autosave cadence rather than tick frequency
     * and is safe to surface through CDC. {@code null} when the tenant does not
     * surface this column. For a currently-online character the value is stale
     * by up to one autosave interval; platform consumers wanting a live figure
     * compose {@code value + (now − loginAt)} from the presence stream.
     */
    public @Nullable Long getOnlineTimeSeconds() {
        return onlineTimeSeconds;
    }

    /**
     * Current hero status — {@code true} when the character is a recognized
     * hero in the active Olympiad cycle (source {@code heroes.played = 1}).
     * {@code null} when the tenant does not surface hero status. Historical
     * crownings (who became hero, when, in which class / cycle) are carried by
     * the discrete {@code HeroGrantedEvent} on the {@code olympiad} event
     * family, not here.
     */
    public @Nullable Boolean getHero() {
        return hero;
    }

    /**
     * Whether experience gain is blocked for this character — source
     * legacy char-var {@code blockedEXP@} ({@code "1"} = blocked,
     * {@code "0"}/absent = allowed). {@code null} when the tenant does not
     * surface this flag.
     */
    public @Nullable Boolean getExpBlocked() {
        return expBlocked;
    }

    /**
     * Gear score of the character's active class — a build-defined numeric
     * "power" rating summed from item base / enchant / attribute / augment,
     * character level, skills and set bonuses. Persisted by the game server only
     * on character store (logout / autosave), so the value is a snapshot at the
     * last save, not a live figure for an online character. {@code null} when the
     * build does not compute gear score or does not surface the column.
     */
    public @Nullable Integer getGearScore() {
        return gearScore;
    }

    /**
     * Character fame (reputation) points; null when the source build reports none.
     */
    public @Nullable Long getFame() {
        return fame;
    }

    /**
     * Opaque GM/access level; numeric text on int-based builds (e.g. "7"), role
     * name on string-role builds; null when not surfaced.
     */
    public @Nullable String getAccessLevel() {
        return accessLevel;
    }

    /**
     * Per-instance re-entry cooldowns — source {@code character_instance_time}
     * (or its tenant equivalent), one entry per {@code (charId, instanceId)}.
     * {@code null} when the tenant does not sync instance cooldowns (no
     * {@code ChildSource} declared); empty list when the tenant syncs them but
     * the character has none.
     */
    public @Nullable List<CharacterInstanceCooldownDbDto> getInstanceCooldowns() {
        return instanceCooldowns;
    }

    /**
     * Active character locks — one entry per in-effect binding derived from
     * build-specific {@code character_variables} rows (bohpts {@code lockIp} /
     * {@code lockHwid} / {@code lockItem}). Each entry's {@code lockType} is a
     * {@link WellKnownCharacterLockTypes} value. {@code null} when the tenant does
     * not sync locks; empty list when the tenant syncs them but the character has
     * no active lock.
     */
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
                .sex(sex)
                .race(race)
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
                && sex == that.sex
                && race == that.race
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
                sex,
                race,
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
                + ", sex=" + sex
                + ", race=" + race
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
        private @Nullable CharacterSex sex;
        private @Nullable CharacterRace race;
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

        public Builder sex(@Nullable CharacterSex sex) {
            this.sex = sex;
            return this;
        }

        public Builder race(@Nullable CharacterRace race) {
            this.race = race;
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
                    sex,
                    race,
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
