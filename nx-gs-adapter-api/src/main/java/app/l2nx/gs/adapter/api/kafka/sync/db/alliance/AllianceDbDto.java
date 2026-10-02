package app.l2nx.gs.adapter.api.kafka.sync.db.alliance;

import java.util.Arrays;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for one alliance, payload of {@code SyncEvent<AllianceDbDto>}. {@code id} and {@code name} are required.
 * {@code icon} is the crest as PNG bytes, same as {@code ClanDbDto.icon}.
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
