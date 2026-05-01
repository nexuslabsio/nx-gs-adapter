package app.l2nx.gs.adapter.api.kafka.sync.db.character;

import app.l2nx.gs.adapter.api.domain.character.CharacterClass;
import app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore;
import app.l2nx.gs.adapter.api.domain.character.CharacterRace;
import app.l2nx.gs.adapter.api.domain.character.CharacterSex;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wire DTO for one player character, payload of
 * {@code SyncEvent<CharacterDto>} on the platform-supplied per-tenant
 * character sync topic (e.g. {@code bohpts.gs.sync.characters}).
 *
 * <p>Only the primary key {@code id} (source-side {@code charId}) is
 * required; everything else is optional. Different tenants populate
 * different subsets depending on which columns exist in their schema and
 * which the tenant chose to surface — schema providers control this via
 * {@code PrimarySource.hashedColumns()} and what they put into the row in
 * {@code mapRow()}.</p>
 *
 * <p>Sentinel mapping: most game-server schemas use {@code 0} as the
 * "no clan" sentinel in {@code characters.clanid}. Schema providers
 * translate sentinel-zero (and source SQL NULL) to {@code null} when
 * populating {@code clanId}; platform consumers see explicit nulls.</p>
 *
 * <p>Volatile state ({@code curHp}/{@code curMp}/{@code x}/{@code y}/
 * {@code z}/{@code exp}/{@code onlinetime}/{@code lastAccess} and similar
 * tick-frequency fields) is intentionally not modeled — including such
 * fields in a poll-based CDC hash would cause an UPDATE storm for every
 * online character on every cycle. Real-time state belongs on a separate
 * event channel.</p>
 */
public final class CharacterDto {

    private final long id;
    private final @Nullable String name;
    private final @Nullable String title;
    private final @Nullable Integer level;
    private final @Nullable CharacterSex sex;
    private final @Nullable CharacterRace race;
    private final @Nullable CharacterClass classId;
    private final @Nullable CharacterClass baseClassId;
    private final @Nullable List<CharacterSubclassDto> subclasses;
    private final @Nullable CharacterPrivateStore privateStore;
    private final @Nullable Long clanId;
    private final @Nullable Integer pvpCounter;
    private final @Nullable Integer pkCounter;
    private final @Nullable Integer karma;

    public CharacterDto(long id,
                        @Nullable String name,
                        @Nullable String title,
                        @Nullable Integer level,
                        @Nullable CharacterSex sex,
                        @Nullable CharacterRace race,
                        @Nullable CharacterClass classId,
                        @Nullable CharacterClass baseClassId,
                        @Nullable List<CharacterSubclassDto> subclasses,
                        @Nullable CharacterPrivateStore privateStore,
                        @Nullable Long clanId,
                        @Nullable Integer pvpCounter,
                        @Nullable Integer pkCounter,
                        @Nullable Integer karma) {
        this.id = id;
        this.name = name;
        this.title = title;
        this.level = level;
        this.sex = sex;
        this.race = race;
        this.classId = classId;
        this.baseClassId = baseClassId;
        this.subclasses = subclasses == null ? null : Collections.unmodifiableList(subclasses);
        this.privateStore = privateStore;
        this.clanId = clanId;
        this.pvpCounter = pvpCounter;
        this.pkCounter = pkCounter;
        this.karma = karma;
    }

    /**
     * Primary key — source {@code charId}, {@code NOT NULL}.
     */
    public long getId() {
        return id;
    }

    /**
     * Character name — source {@code char_name}, {@code NOT NULL} on the
     * source side. {@code null} when the tenant does not surface this column.
     */
    public @Nullable String getName() {
        return name;
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
     * Character subclasses, ordered as the schema provider's
     * {@code mapEntity} produced them (no platform-side ordering contract).
     * {@code null} when the tenant does not sync subclasses (no
     * {@code ChildSource} declared); empty list when the tenant syncs
     * subclasses but the character has none.
     */
    public @Nullable List<CharacterSubclassDto> getSubclasses() {
        return subclasses;
    }

    /**
     * Active private-store mode. {@code null} when the character has no
     * store open (or only a transient menu-pending state), or when the
     * tenant does not surface this datum.
     */
    public @Nullable CharacterPrivateStore getPrivateStore() {
        return privateStore;
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

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .name(name)
                .title(title)
                .level(level)
                .sex(sex)
                .race(race)
                .classId(classId)
                .baseClassId(baseClassId)
                .subclasses(subclasses)
                .privateStore(privateStore)
                .clanId(clanId)
                .pvpCounter(pvpCounter)
                .pkCounter(pkCounter)
                .karma(karma);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CharacterDto)) return false;
        CharacterDto that = (CharacterDto) o;
        return id == that.id
                && Objects.equals(name, that.name)
                && Objects.equals(title, that.title)
                && Objects.equals(level, that.level)
                && sex == that.sex
                && race == that.race
                && classId == that.classId
                && baseClassId == that.baseClassId
                && Objects.equals(subclasses, that.subclasses)
                && privateStore == that.privateStore
                && Objects.equals(clanId, that.clanId)
                && Objects.equals(pvpCounter, that.pvpCounter)
                && Objects.equals(pkCounter, that.pkCounter)
                && Objects.equals(karma, that.karma);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, title, level, sex, race,
                classId, baseClassId, subclasses, privateStore,
                clanId, pvpCounter, pkCounter, karma);
    }

    @Override
    public String toString() {
        return "CharacterDto[id=" + id
                + ", name=" + name
                + ", title=" + title
                + ", level=" + level
                + ", sex=" + sex
                + ", race=" + race
                + ", classId=" + classId
                + ", baseClassId=" + baseClassId
                + ", subclasses=" + subclasses
                + ", privateStore=" + privateStore
                + ", clanId=" + clanId
                + ", pvpCounter=" + pvpCounter
                + ", pkCounter=" + pkCounter
                + ", karma=" + karma + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable String name;
        private @Nullable String title;
        private @Nullable Integer level;
        private @Nullable CharacterSex sex;
        private @Nullable CharacterRace race;
        private @Nullable CharacterClass classId;
        private @Nullable CharacterClass baseClassId;
        private @Nullable List<CharacterSubclassDto> subclasses;
        private @Nullable CharacterPrivateStore privateStore;
        private @Nullable Long clanId;
        private @Nullable Integer pvpCounter;
        private @Nullable Integer pkCounter;
        private @Nullable Integer karma;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder name(@Nullable String name) {
            this.name = name;
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

        public Builder subclasses(@Nullable List<CharacterSubclassDto> subclasses) {
            this.subclasses = subclasses;
            return this;
        }

        public Builder privateStore(@Nullable CharacterPrivateStore privateStore) {
            this.privateStore = privateStore;
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

        public CharacterDto build() {
            return new CharacterDto(id, name, title, level, sex, race,
                    classId, baseClassId, subclasses, privateStore,
                    clanId, pvpCounter, pkCounter, karma);
        }
    }
}
