package app.l2nx.gs.adapter.api.domain.stat;

/**
 * Shape discriminator for a typed characteristic value (the {@code type} of a
 * {@code StatDto}). Tells a consumer how to parse the accompanying {@code value}
 * payload; it is deliberately small and shape-only — the concrete vocabulary of an
 * {@link #ENUM} value is identified by the stat's key, not by a per-vocabulary member.
 *
 * <p>Build-agnostic: emitted as {@code name()} (UPPER_SNAKE), consistent with the
 * platform's enum-like-vocab convention. The typed-characteristics model is assembled
 * read-side (in the wiki), so this enum is the source of truth for documenting the
 * {@code type} discriminator, not a wire-key type.</p>
 *
 * <ul>
 *   <li>{@link #NUMBER} — a number.</li>
 *   <li>{@link #STRING} — free text shown as-is.</li>
 *   <li>{@link #BOOLEAN} — a boolean.</li>
 *   <li>{@link #ENUM} — a token of a closed canonical vocabulary (which one is fixed by the key).</li>
 *   <li>{@link #ARRAY} — a homogeneous array of primitives / enum tokens.</li>
 *   <li>{@link #DURATION} — an ISO-8601 duration string ({@code PT3S}).</li>
 *   <li>{@link #ITEM_TEMPLATE_COUNT} — an item-template reference plus a count.</li>
 * </ul>
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
