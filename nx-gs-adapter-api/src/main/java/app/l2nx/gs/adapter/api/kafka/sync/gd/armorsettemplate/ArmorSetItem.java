package app.l2nx.gs.adapter.api.kafka.sync.gd.armorsettemplate;

import java.util.Objects;

/**
 * One item that fills a slot of an {@link ArmorSetTemplate}. A slot may have several
 * alternative items (each its own row). {@code slot} is the canonical UPPER_SNAKE slot
 * vocabulary ({@code CHEST}/{@code LEGS}/{@code HEAD}/{@code GLOVES}/{@code FEET}/
 * {@code SHIELD}); {@code itemTemplateId} is the FK to the item-template entity.
 */
public final class ArmorSetItem {

    private final String slot;
    private final int itemTemplateId;

    public ArmorSetItem(String slot, int itemTemplateId) {
        this.slot = Objects.requireNonNull(slot, "slot");
        this.itemTemplateId = itemTemplateId;
    }

    /**
     * Slot this item fills: {@code CHEST}/{@code LEGS}/{@code HEAD}/{@code GLOVES}/
     * {@code FEET}/{@code SHIELD}.
     */
    public String getSlot() {
        return slot;
    }

    public int getItemTemplateId() {
        return itemTemplateId;
    }

    public Builder toBuilder() {
        return new Builder()
                .slot(slot)
                .itemTemplateId(itemTemplateId);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ArmorSetItem)) return false;
        ArmorSetItem that = (ArmorSetItem) o;
        return itemTemplateId == that.itemTemplateId && Objects.equals(slot, that.slot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(slot, itemTemplateId);
    }

    @Override
    public String toString() {
        return "ArmorSetItem[slot=" + slot + ", itemTemplateId=" + itemTemplateId + "]";
    }

    public static final class Builder {
        private String slot;
        private int itemTemplateId;

        public Builder slot(String slot) {
            this.slot = slot;
            return this;
        }

        public Builder itemTemplateId(int itemTemplateId) {
            this.itemTemplateId = itemTemplateId;
            return this;
        }

        public ArmorSetItem build() {
            return new ArmorSetItem(slot, itemTemplateId);
        }
    }
}
