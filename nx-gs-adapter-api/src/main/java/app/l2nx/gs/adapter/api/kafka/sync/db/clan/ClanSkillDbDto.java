package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import java.util.Objects;

/** Wire DTO for one {@code clan_skills} row, carried in {@link ClanDbDto#getSkills()}. */
public final class ClanSkillDbDto {

    private final int id;
    private final int level;

    public ClanSkillDbDto(int id, int level) {
        this.id = id;
        this.level = level;
    }

    public int getId() {
        return id;
    }

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
