package app.l2nx.gs.adapter.api.kafka.sync.db.alliance;

import org.jspecify.annotations.Nullable;

import java.util.Arrays;
import java.util.Objects;

/**
 * Wire DTO for one alliance, payload of {@code SyncEvent<AllianceDto>} on
 * the per-tenant alliance sync topic.
 *
 * <p>L2-derived schemas denormalize alliance state across {@code clan_data}
 * (every member clan carries {@code ally_id}, {@code ally_name},
 * {@code ally_crest_id}); schema providers project this into a per-alliance
 * shape via a view. Builds with a physical {@code ally_data}-like table
 * emit the same wire shape from a plain table. {@code icon} carries the
 * alliance crest as PNG bytes — same convention as {@code ClanDto.icon}.</p>
 */
public final class AllianceDto {

    private final long allyId;
    private final String allyName;
    private final byte @Nullable [] icon;

    public AllianceDto(long allyId, String allyName, byte @Nullable [] icon) {
        this.allyId = allyId;
        this.allyName = allyName;
        this.icon = icon;
    }

    public long getAllyId() {
        return allyId;
    }

    public String getAllyName() {
        return allyName;
    }

    public byte @Nullable [] getIcon() {
        return icon;
    }

    public Builder toBuilder() {
        return new Builder()
                .allyId(allyId)
                .allyName(allyName)
                .icon(icon);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AllianceDto)) return false;
        AllianceDto that = (AllianceDto) o;
        return allyId == that.allyId
                && Objects.equals(allyName, that.allyName)
                && Arrays.equals(icon, that.icon);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(allyId, allyName);
        result = 31 * result + Arrays.hashCode(icon);
        return result;
    }

    @Override
    public String toString() {
        return "AllianceDto[allyId=" + allyId
                + ", allyName=" + allyName
                + ", icon=" + (icon == null ? "null" : "byte[" + icon.length + "]") + "]";
    }

    public static final class Builder {
        private long allyId;
        private String allyName;
        private byte @Nullable [] icon;

        public Builder allyId(long allyId) {
            this.allyId = allyId;
            return this;
        }

        public Builder allyName(String allyName) {
            this.allyName = allyName;
            return this;
        }

        public Builder icon(byte @Nullable [] icon) {
            this.icon = icon;
            return this;
        }

        public AllianceDto build() {
            return new AllianceDto(allyId, allyName, icon);
        }
    }
}
