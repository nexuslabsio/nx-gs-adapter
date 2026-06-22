package app.l2nx.gs.adapter.api.kafka.sync.db.alliance;

import java.util.Arrays;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one alliance, payload of {@code SyncEvent<AllianceDbDto>} on
 * the per-tenant alliance sync topic.
 *
 * <p>Required: {@link #getId() id} (source-side {@code ally_id}) and
 * {@link #getName() name} (source-side {@code ally_name}). Schema providers
 * MUST drop dirty rows that lack either rather than ship placeholders.</p>
 *
 * <p>L2-derived schemas denormalize alliance state across {@code clan_data}
 * (every member clan carries {@code ally_id}, {@code ally_name},
 * {@code ally_crest_id}); schema providers project this into a per-alliance
 * shape via a view. Builds with a physical {@code ally_data}-like table
 * emit the same wire shape from a plain table. {@code icon} carries the
 * alliance crest as PNG bytes — same convention as {@code ClanDbDto.icon}.</p>
 */
public final class AllianceDbDto {

    private final long id;
    private final String name;
    private final byte @Nullable [] icon;

    public AllianceDbDto(long id, String name, byte @Nullable [] icon) {
        this.id = id;
        this.name = Objects.requireNonNull(name, "AllianceDbDto.name is required");
        this.icon = icon;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public byte @Nullable [] getIcon() {
        return icon;
    }

    public Builder toBuilder() {
        return new Builder().id(id).name(name).icon(icon);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AllianceDbDto)) return false;
        AllianceDbDto that = (AllianceDbDto) o;
        return id == that.id && name.equals(that.name) && Arrays.equals(icon, that.icon);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(id, name);
        result = 31 * result + Arrays.hashCode(icon);
        return result;
    }

    @Override
    public String toString() {
        return "AllianceDbDto[id=" + id
                + ", name=" + name
                + ", icon=" + (icon == null ? "null" : "byte[" + icon.length + "]") + "]";
    }

    public static final class Builder {
        private long id;
        private @Nullable String name;
        private byte @Nullable [] icon;

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder name(String name) {
            this.name = name;
            return this;
        }

        public Builder icon(byte @Nullable [] icon) {
            this.icon = icon;
            return this;
        }

        public AllianceDbDto build() {
            return new AllianceDbDto(id, name, icon);
        }
    }
}
