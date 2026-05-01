package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wire DTO for one clan, payload of {@code SyncEvent<ClanDto>} on the
 * platform-supplied per-tenant clan sync topic
 * (e.g. {@code bohpts.gs.sync.clans}).
 *
 * <p>Field types mirror DB nullability: primitives for {@code NOT NULL} columns,
 * boxed for nullable. Gson serializes both identically (both render as JSON
 * numbers); the type carries the nullability contract.</p>
 *
 * <p>Sentinel mapping: most game-server schemas use {@code 0} as the
 * "no leader" / "no ally" sentinel in {@code clan_data.leader_id} /
 * {@code ally_id}. Schema providers translate sentinel-zero to {@code null}
 * when populating these fields ({@code Long leaderId / allyId}); platform
 * consumers see explicit nulls.</p>
 *
 * <p>The {@code skills} list aggregates child rows from the tenant's
 * {@code clan_skills}-equivalent table assembled by the schema provider's
 * {@code mapEntity}. {@code null} when the tenant does not sync skills
 * at all (no {@code ChildSource} declared for skills); empty list when the
 * tenant syncs skills but the clan has none. Gson's default
 * {@code serializeNulls=false} omits the field from JSON when {@code null},
 * so the wire shape unambiguously distinguishes "feature not synced" from
 * "feature synced, value empty".</p>
 */
public final class ClanDto {

    private final long clanId;
    private final String clanName;
    private final int clanLevel;
    private final @Nullable Long leaderId;
    private final @Nullable Long allyId;
    private final @Nullable List<ClanSkillDto> skills;

    public ClanDto(long clanId, String clanName, int clanLevel,
                   @Nullable Long leaderId, @Nullable Long allyId,
                   @Nullable List<ClanSkillDto> skills) {
        this.clanId = clanId;
        this.clanName = clanName;
        this.clanLevel = clanLevel;
        this.leaderId = leaderId;
        this.allyId = allyId;
        this.skills = skills == null ? null : Collections.unmodifiableList(skills);
    }

    /**
     * Primary key — {@code NOT NULL}.
     */
    public long getClanId() {
        return clanId;
    }

    /**
     * {@code NOT NULL}.
     */
    public String getClanName() {
        return clanName;
    }

    /**
     * {@code NOT NULL}; source default {@code 0}.
     */
    public int getClanLevel() {
        return clanLevel;
    }

    /**
     * {@code null} when source {@code leader_id = 0} (the conventional
     * "no leader" sentinel).
     */
    public @Nullable Long getLeaderId() {
        return leaderId;
    }

    /**
     * {@code null} when source {@code ally_id = 0} (the conventional
     * "no ally" sentinel).
     */
    public @Nullable Long getAllyId() {
        return allyId;
    }

    /**
     * Clan skills, ordered as the schema provider's {@code mapEntity}
     * produced them (no platform-side ordering contract). {@code null} when
     * the tenant does not sync skills (no {@code ChildSource} declared);
     * empty list when the tenant syncs skills but the clan has none.
     */
    public @Nullable List<ClanSkillDto> getSkills() {
        return skills;
    }

    public Builder toBuilder() {
        return new Builder()
                .clanId(clanId)
                .clanName(clanName)
                .clanLevel(clanLevel)
                .leaderId(leaderId)
                .allyId(allyId)
                .skills(skills);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClanDto)) return false;
        ClanDto that = (ClanDto) o;
        return clanId == that.clanId
                && clanLevel == that.clanLevel
                && Objects.equals(clanName, that.clanName)
                && Objects.equals(leaderId, that.leaderId)
                && Objects.equals(allyId, that.allyId)
                && Objects.equals(skills, that.skills);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clanId, clanName, clanLevel, leaderId, allyId, skills);
    }

    @Override
    public String toString() {
        return "ClanDto[clanId=" + clanId
                + ", clanName=" + clanName
                + ", clanLevel=" + clanLevel
                + ", leaderId=" + leaderId
                + ", allyId=" + allyId
                + ", skills=" + skills + "]";
    }

    public static final class Builder {
        private long clanId;
        private String clanName;
        private int clanLevel;
        private @Nullable Long leaderId;
        private @Nullable Long allyId;
        private @Nullable List<ClanSkillDto> skills;

        public Builder clanId(long clanId) {
            this.clanId = clanId;
            return this;
        }

        public Builder clanName(String clanName) {
            this.clanName = clanName;
            return this;
        }

        public Builder clanLevel(int clanLevel) {
            this.clanLevel = clanLevel;
            return this;
        }

        public Builder leaderId(@Nullable Long leaderId) {
            this.leaderId = leaderId;
            return this;
        }

        public Builder allyId(@Nullable Long allyId) {
            this.allyId = allyId;
            return this;
        }

        public Builder skills(@Nullable List<ClanSkillDto> skills) {
            this.skills = skills;
            return this;
        }

        public ClanDto build() {
            return new ClanDto(clanId, clanName, clanLevel, leaderId, allyId, skills);
        }
    }
}
