package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import java.util.Objects;

/**
 * Wire DTO for one row of {@code clan_skills} (or its tenant-equivalent),
 * carried inside {@link ClanDto#getSkills()}.
 *
 * <p>Only the identifying-and-versioning pair is surfaced on the wire:
 * {@code skillId} (which skill) and {@code skillLevel} (current level).
 * Other source-side columns ({@code skill_name}, {@code sub_pledge_id}) are
 * intentionally not modeled — they are display / partitioning details that
 * platform consumers don't need.</p>
 */
public final class ClanSkillDto {

    private final int skillId;
    private final int skillLevel;

    public ClanSkillDto(int skillId, int skillLevel) {
        this.skillId = skillId;
        this.skillLevel = skillLevel;
    }

    /**
     * Skill identifier — {@code NOT NULL} on the source side.
     */
    public int getSkillId() {
        return skillId;
    }

    /**
     * Skill level — {@code NOT NULL} on the source side; source default
     * {@code 0}.
     */
    public int getSkillLevel() {
        return skillLevel;
    }

    public Builder toBuilder() {
        return new Builder().skillId(skillId).skillLevel(skillLevel);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClanSkillDto)) return false;
        ClanSkillDto that = (ClanSkillDto) o;
        return skillId == that.skillId && skillLevel == that.skillLevel;
    }

    @Override
    public int hashCode() {
        return Objects.hash(skillId, skillLevel);
    }

    @Override
    public String toString() {
        return "ClanSkillDto[skillId=" + skillId + ", skillLevel=" + skillLevel + "]";
    }

    public static final class Builder {
        private int skillId;
        private int skillLevel;

        public Builder skillId(int skillId) {
            this.skillId = skillId;
            return this;
        }

        public Builder skillLevel(int skillLevel) {
            this.skillLevel = skillLevel;
            return this;
        }

        public ClanSkillDto build() {
            return new ClanSkillDto(skillId, skillLevel);
        }
    }
}
