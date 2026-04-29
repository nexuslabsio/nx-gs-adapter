package app.l2nx.gs.adapter.api.kafka.sync.db;

import java.util.Objects;

/**
 * Wire DTO for one clan row, payload of {@code SyncEvent<ClanDto>} on the
 * platform-supplied per-tenant clan sync topic
 * (e.g. {@code bohpts.gs.sync.clans}).
 *
 * <p>Field types mirror DB nullability: primitives for {@code NOT NULL} columns,
 * boxed for nullable. Gson serializes both identically (both render as JSON
 * numbers); the type carries the nullability contract.</p>
 *
 * <p>Sentinel mapping: bohpts (and most L2J forks) use {@code 0} as the
 * "no leader" / "no ally" sentinel in {@code clan_data.leader_id} /
 * {@code ally_id}. Schema providers translate sentinel-zero to {@code null}
 * when populating these fields ({@code Long leaderId / allyId}); platform
 * consumers see explicit nulls.</p>
 */
public final class ClanDto {

    private final long clanId;
    private final String clanName;
    private final int clanLevel;
    private final Long leaderId;
    private final Long allyId;

    public ClanDto(long clanId, String clanName, int clanLevel, Long leaderId, Long allyId) {
        this.clanId = clanId;
        this.clanName = clanName;
        this.clanLevel = clanLevel;
        this.leaderId = leaderId;
        this.allyId = allyId;
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
     * {@code null} when source {@code leader_id = 0} (L2J convention).
     */
    public Long getLeaderId() {
        return leaderId;
    }

    /**
     * {@code null} when source {@code ally_id = 0} (L2J convention).
     */
    public Long getAllyId() {
        return allyId;
    }

    public Builder toBuilder() {
        return new Builder()
                .clanId(clanId)
                .clanName(clanName)
                .clanLevel(clanLevel)
                .leaderId(leaderId)
                .allyId(allyId);
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
                && Objects.equals(allyId, that.allyId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clanId, clanName, clanLevel, leaderId, allyId);
    }

    @Override
    public String toString() {
        return "ClanDto[clanId=" + clanId
                + ", clanName=" + clanName
                + ", clanLevel=" + clanLevel
                + ", leaderId=" + leaderId
                + ", allyId=" + allyId + "]";
    }

    public static final class Builder {
        private long clanId;
        private String clanName;
        private int clanLevel;
        private Long leaderId;
        private Long allyId;

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

        public Builder leaderId(Long leaderId) {
            this.leaderId = leaderId;
            return this;
        }

        public Builder allyId(Long allyId) {
            this.allyId = allyId;
            return this;
        }

        public ClanDto build() {
            return new ClanDto(clanId, clanName, clanLevel, leaderId, allyId);
        }
    }
}
