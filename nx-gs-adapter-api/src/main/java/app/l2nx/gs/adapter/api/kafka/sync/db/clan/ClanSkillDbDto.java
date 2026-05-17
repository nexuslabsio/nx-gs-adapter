package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import java.util.Objects;

/**
 * Wire DTO for one row of {@code clan_skills} (or its tenant-equivalent),
 * carried inside {@link ClanDbDto#getSkills()}.
 *
 * <p>Only the identifying-and-versioning pair is surfaced on the wire:
 * {@code id} (which skill, source {@code skill_id}) and {@code level}
 * (current level). Other source-side columns ({@code skill_name},
 * {@code sub_pledge_id}) are intentionally not modeled — they are display /
 * partitioning details that platform consumers don't need.</p>
 */
public final class ClanSkillDbDto {

    private final int id;
    private final int level;

    public ClanSkillDbDto(int id, int level) {
        this.id = id;
        this.level = level;
    }

    /**
     * Skill identifier — source {@code skill_id}, {@code NOT NULL}.
     */
    public int getId() {
        return id;
    }

    /**
     * Skill level — {@code NOT NULL} on the source side; source default
     * {@code 0}.
     */
    public int getLevel() {
        return level;
    }

    public Builder toBuilder() {
        return new Builder().id(id).level(level);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClanSkillDbDto)) return false;
        ClanSkillDbDto that = (ClanSkillDbDto) o;
        return id == that.id && level == that.level;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, level);
    }

    @Override
    public String toString() {
        return "ClanSkillDbDto[id=" + id + ", level=" + level + "]";
    }

    public static final class Builder {
        private int id;
        private int level;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder level(int level) {
            this.level = level;
            return this;
        }

        public ClanSkillDbDto build() {
            return new ClanSkillDbDto(id, level);
        }
    }
}
