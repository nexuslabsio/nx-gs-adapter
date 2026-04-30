package app.l2nx.gs.adapter.api.kafka.sync.db;

import app.l2nx.gs.adapter.api.domain.Race;
import app.l2nx.gs.adapter.api.domain.Sex;
import org.jspecify.annotations.Nullable;

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
 * <p>Sentinel mapping: bohpts (and most L2J forks) use {@code 0} as the
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
    private final @Nullable Sex sex;
    private final @Nullable Race race;
    private final @Nullable Long clanId;
    private final @Nullable Integer pvpCounter;
    private final @Nullable Integer pkCounter;
    private final @Nullable Integer karma;

    public CharacterDto(long id,
                        @Nullable String name,
                        @Nullable String title,
                        @Nullable Integer level,
                        @Nullable Sex sex,
                        @Nullable Race race,
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
    public @Nullable Sex getSex() {
        return sex;
    }

    /**
     * Character race.
     */
    public @Nullable Race getRace() {
        return race;
    }

    /**
     * Clan membership. {@code null} when the source {@code clanid = 0}
     * (L2J convention) or SQL NULL or when the tenant does not surface
     * this column.
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
                && Objects.equals(clanId, that.clanId)
                && Objects.equals(pvpCounter, that.pvpCounter)
                && Objects.equals(pkCounter, that.pkCounter)
                && Objects.equals(karma, that.karma);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, title, level, sex, race,
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
        private @Nullable Sex sex;
        private @Nullable Race race;
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

        public Builder sex(@Nullable Sex sex) {
            this.sex = sex;
            return this;
        }

        public Builder race(@Nullable Race race) {
            this.race = race;
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
                    clanId, pvpCounter, pkCounter, karma);
        }
    }
}
