package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wire DTO for one clan, payload of {@code SyncEvent<ClanDto>} on the
 * per-tenant clan sync topic.
 *
 * <p>Schema providers translate source sentinels (typically
 * {@code leader_id} / {@code ally_id} = 0) to {@code null} so the platform
 * sees explicit absence. {@code skills} is {@code null} when the tenant
 * does not sync skills at all (no {@code ChildSource} declared), empty
 * list when the clan has none — Gson's default
 * {@code serializeNulls=false} preserves that distinction on the wire.
 * {@code icon} carries the clan crest as already-decoded PNG bytes
 * (schema provider converts its native blob format in {@code mapEntity});
 * {@code null} when no crest is synced or the source row has no reference.</p>
 */
public final class ClanDto {

    private final long clanId;
    private final String clanName;
    private final int clanLevel;
    private final @Nullable Long leaderId;
    private final @Nullable Long allyId;
    private final @Nullable List<ClanSkillDto> skills;
    private final byte @Nullable [] icon;

    public ClanDto(long clanId, String clanName, int clanLevel,
                   @Nullable Long leaderId, @Nullable Long allyId,
                   @Nullable List<ClanSkillDto> skills,
                   byte @Nullable [] icon) {
        this.clanId = clanId;
        this.clanName = clanName;
        this.clanLevel = clanLevel;
        this.leaderId = leaderId;
        this.allyId = allyId;
        this.skills = skills == null ? null : Collections.unmodifiableList(skills);
        this.icon = icon;
    }

    public long getClanId() {
        return clanId;
    }

    public String getClanName() {
        return clanName;
    }

    public int getClanLevel() {
        return clanLevel;
    }

    public @Nullable Long getLeaderId() {
        return leaderId;
    }

    public @Nullable Long getAllyId() {
        return allyId;
    }

    public @Nullable List<ClanSkillDto> getSkills() {
        return skills;
    }

    public byte @Nullable [] getIcon() {
        return icon;
    }

    public Builder toBuilder() {
        return new Builder()
                .clanId(clanId)
                .clanName(clanName)
                .clanLevel(clanLevel)
                .leaderId(leaderId)
                .allyId(allyId)
                .skills(skills)
                .icon(icon);
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
                && Objects.equals(skills, that.skills)
                && Arrays.equals(icon, that.icon);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(clanId, clanName, clanLevel, leaderId, allyId, skills);
        result = 31 * result + Arrays.hashCode(icon);
        return result;
    }

    @Override
    public String toString() {
        return "ClanDto[clanId=" + clanId
                + ", clanName=" + clanName
                + ", clanLevel=" + clanLevel
                + ", leaderId=" + leaderId
                + ", allyId=" + allyId
                + ", skills=" + skills
                + ", icon=" + (icon == null ? "null" : "byte[" + icon.length + "]") + "]";
    }

    public static final class Builder {
        private long clanId;
        private String clanName;
        private int clanLevel;
        private @Nullable Long leaderId;
        private @Nullable Long allyId;
        private @Nullable List<ClanSkillDto> skills;
        private byte @Nullable [] icon;

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

        public Builder icon(byte @Nullable [] icon) {
            this.icon = icon;
            return this;
        }

        public ClanDto build() {
            return new ClanDto(clanId, clanName, clanLevel, leaderId, allyId, skills, icon);
        }
    }
}
