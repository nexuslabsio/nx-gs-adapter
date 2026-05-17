package app.l2nx.gs.adapter.api.kafka.sync.db.clan;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Wire DTO for one clan, payload of {@code SyncEvent<ClanDbDto>} on the
 * per-tenant clan sync topic.
 *
 * <p>Required: {@link #getId() id} (source-side {@code clan_id}) and
 * {@link #getName() name} (source-side {@code clan_name}). Schema
 * providers MUST drop dirty rows that lack either rather than ship
 * placeholders. {@link #getLevel() level} is optional — historical schemas
 * leave it unset on freshly-created clans before the next save tick.</p>
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
public final class ClanDbDto {

    private final long id;
    private final String name;
    private final @Nullable Integer level;
    private final @Nullable Long leaderId;
    private final @Nullable Long allyId;
    private final @Nullable List<ClanSkillDbDto> skills;
    private final byte @Nullable [] icon;

    public ClanDbDto(long id, String name, @Nullable Integer level,
                     @Nullable Long leaderId, @Nullable Long allyId,
                     @Nullable List<ClanSkillDbDto> skills,
                     byte @Nullable [] icon) {
        this.id = id;
        this.name = Objects.requireNonNull(name, "ClanDbDto.name is required");
        this.level = level;
        this.leaderId = leaderId;
        this.allyId = allyId;
        this.skills = skills == null ? null : Collections.unmodifiableList(skills);
        this.icon = icon;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public @Nullable Integer getLevel() {
        return level;
    }

    public @Nullable Long getLeaderId() {
        return leaderId;
    }

    public @Nullable Long getAllyId() {
        return allyId;
    }

    public @Nullable List<ClanSkillDbDto> getSkills() {
        return skills;
    }

    public byte @Nullable [] getIcon() {
        return icon;
    }

    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .name(name)
                .level(level)
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
        if (!(o instanceof ClanDbDto)) return false;
        ClanDbDto that = (ClanDbDto) o;
        return id == that.id
                && Objects.equals(level, that.level)
                && name.equals(that.name)
                && Objects.equals(leaderId, that.leaderId)
                && Objects.equals(allyId, that.allyId)
                && Objects.equals(skills, that.skills)
                && Arrays.equals(icon, that.icon);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, name, level, leaderId, allyId, skills);
        result = 31 * result + Arrays.hashCode(icon);
        return result;
    }

    @Override
    public String toString() {
        return "ClanDbDto[id=" + id
                + ", name=" + name
                + ", level=" + level
                + ", leaderId=" + leaderId
                + ", allyId=" + allyId
                + ", skills=" + skills
                + ", icon=" + (icon == null ? "null" : "byte[" + icon.length + "]") + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable String name;
        private @Nullable Integer level;
        private @Nullable Long leaderId;
        private @Nullable Long allyId;
        private @Nullable List<ClanSkillDbDto> skills;
        private byte @Nullable [] icon;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder level(@Nullable Integer level) {
            this.level = level;
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

        public Builder skills(@Nullable List<ClanSkillDbDto> skills) {
            this.skills = skills;
            return this;
        }

        public Builder icon(byte @Nullable [] icon) {
            this.icon = icon;
            return this;
        }

        public ClanDbDto build() {
            return new ClanDbDto(id, name, level, leaderId, allyId, skills, icon);
        }
    }
}
