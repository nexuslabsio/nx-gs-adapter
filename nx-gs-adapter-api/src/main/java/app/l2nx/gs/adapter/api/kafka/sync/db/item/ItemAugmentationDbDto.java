package app.l2nx.gs.adapter.api.kafka.sync.db.item;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Wire DTO for an item's augmentation, carried in {@link ItemDbDto#getAugmentation()}.
 * Option ids resolve against the platform gd option catalog.
 */
public final class ItemAugmentationDbDto {

    private final int option1Id;
    private final @Nullable Integer option2Id;

    public ItemAugmentationDbDto(int option1Id, @Nullable Integer option2Id) {
        this.option1Id = option1Id;
        this.option2Id = option2Id;
    }

    public int getOption1Id() {
        return option1Id;
    }

    /** Null when the item has only the low-slot option. */
    public @Nullable Integer getOption2Id() {
        return option2Id;
    }

    public Builder toBuilder() {
        return new Builder().option1Id(option1Id).option2Id(option2Id);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemAugmentationDbDto)) return false;
        ItemAugmentationDbDto that = (ItemAugmentationDbDto) o;
        return option1Id == that.option1Id && Objects.equals(option2Id, that.option2Id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(option1Id, option2Id);
    }

    @Override
    public String toString() {
        return "ItemAugmentationDbDto[option1Id=" + option1Id + ", option2Id=" + option2Id + "]";
    }

    public static final class Builder {
        private int option1Id;
        private @Nullable Integer option2Id;

        public Builder option1Id(int option1Id) {
            this.option1Id = option1Id;
            return this;
        }

        public Builder option2Id(@Nullable Integer option2Id) {
            this.option2Id = option2Id;
            return this;
        }

        public ItemAugmentationDbDto build() {
            return new ItemAugmentationDbDto(option1Id, option2Id);
        }
    }
}
