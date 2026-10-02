package app.l2nx.gs.adapter.api.kafka.sync.gd.itemtemplate;

import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * Tri-state ({@code null} = unknown) trade / storage / permission flags of an {@link ItemTemplate}.
 * Only flags the server template exposes; client-patch flags ({@code privateStoreSellable}, {@code npcTrade}, ...) are absent.
 */
public final class ItemRestrictions {

    private final @Nullable Boolean tradable;
    private final @Nullable Boolean dropable;
    private final @Nullable Boolean sellable;
    private final @Nullable Boolean destroyable;
    private final @Nullable Boolean warehouseDepositable;
    private final @Nullable Boolean freightable;
    private final @Nullable Boolean olympiadRestricted;
    private final @Nullable Boolean eventRestricted;

    public ItemRestrictions(
            @Nullable Boolean tradable,
            @Nullable Boolean dropable,
            @Nullable Boolean sellable,
            @Nullable Boolean destroyable,
            @Nullable Boolean warehouseDepositable,
            @Nullable Boolean freightable,
            @Nullable Boolean olympiadRestricted,
            @Nullable Boolean eventRestricted) {
        this.tradable = tradable;
        this.dropable = dropable;
        this.sellable = sellable;
        this.destroyable = destroyable;
        this.warehouseDepositable = warehouseDepositable;
        this.freightable = freightable;
        this.olympiadRestricted = olympiadRestricted;
        this.eventRestricted = eventRestricted;
    }

    public @Nullable Boolean getTradable() {
        return tradable;
    }

    public @Nullable Boolean getDropable() {
        return dropable;
    }

    public @Nullable Boolean getSellable() {
        return sellable;
    }

    public @Nullable Boolean getDestroyable() {
        return destroyable;
    }

    /**
     * Personal warehouse only, not the clan warehouse.
     */
    public @Nullable Boolean getWarehouseDepositable() {
        return warehouseDepositable;
    }

    public @Nullable Boolean getFreightable() {
        return freightable;
    }

    public @Nullable Boolean getOlympiadRestricted() {
        return olympiadRestricted;
    }

    public @Nullable Boolean getEventRestricted() {
        return eventRestricted;
    }

    public Builder toBuilder() {
        return new Builder()
                .tradable(tradable)
                .dropable(dropable)
                .sellable(sellable)
                .destroyable(destroyable)
                .warehouseDepositable(warehouseDepositable)
                .freightable(freightable)
                .olympiadRestricted(olympiadRestricted)
                .eventRestricted(eventRestricted);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ItemRestrictions)) return false;
        ItemRestrictions that = (ItemRestrictions) o;
        return Objects.equals(tradable, that.tradable)
                && Objects.equals(dropable, that.dropable)
                && Objects.equals(sellable, that.sellable)
                && Objects.equals(destroyable, that.destroyable)
                && Objects.equals(warehouseDepositable, that.warehouseDepositable)
                && Objects.equals(freightable, that.freightable)
                && Objects.equals(olympiadRestricted, that.olympiadRestricted)
                && Objects.equals(eventRestricted, that.eventRestricted);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                tradable,
                dropable,
                sellable,
                destroyable,
                warehouseDepositable,
                freightable,
                olympiadRestricted,
                eventRestricted);
    }

    @Override
    public String toString() {
        return "ItemRestrictions[tradable=" + tradable + ", dropable=" + dropable + ", sellable=" + sellable + "]";
    }

    public static final class Builder {
        private @Nullable Boolean tradable;
        private @Nullable Boolean dropable;
        private @Nullable Boolean sellable;
        private @Nullable Boolean destroyable;
        private @Nullable Boolean warehouseDepositable;
        private @Nullable Boolean freightable;
        private @Nullable Boolean olympiadRestricted;
        private @Nullable Boolean eventRestricted;

        public Builder tradable(@Nullable Boolean tradable) {
            this.tradable = tradable;
            return this;
        }

        public Builder dropable(@Nullable Boolean dropable) {
            this.dropable = dropable;
            return this;
        }

        public Builder sellable(@Nullable Boolean sellable) {
            this.sellable = sellable;
            return this;
        }

        public Builder destroyable(@Nullable Boolean destroyable) {
            this.destroyable = destroyable;
            return this;
        }

        public Builder warehouseDepositable(@Nullable Boolean warehouseDepositable) {
            this.warehouseDepositable = warehouseDepositable;
            return this;
        }

        public Builder freightable(@Nullable Boolean freightable) {
            this.freightable = freightable;
            return this;
        }

        public Builder olympiadRestricted(@Nullable Boolean olympiadRestricted) {
            this.olympiadRestricted = olympiadRestricted;
            return this;
        }

        public Builder eventRestricted(@Nullable Boolean eventRestricted) {
            this.eventRestricted = eventRestricted;
            return this;
        }

        public ItemRestrictions build() {
            return new ItemRestrictions(
                    tradable,
                    dropable,
                    sellable,
                    destroyable,
                    warehouseDepositable,
                    freightable,
                    olympiadRestricted,
                    eventRestricted);
        }
    }
}
