package app.l2nx.gs.adapter.api.domain.stat;

/**
 * Shape discriminator of a typed characteristic value; says how to parse {@code value}. The vocabulary
 * of an {@link #ENUM} value is fixed by the stat's key. {@link #DURATION} is ISO-8601 ({@code PT3S}).
 */
public enum StatType {
    NUMBER,
    STRING,
    BOOLEAN,
    ENUM,
    ARRAY,
    DURATION,
    ITEM_TEMPLATE_COUNT
}
