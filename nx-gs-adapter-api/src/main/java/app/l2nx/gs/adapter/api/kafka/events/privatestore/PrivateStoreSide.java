package app.l2nx.gs.adapter.api.kafka.events.privatestore;

/**
 * Side of the private-store order book that an offer (or a closed trade)
 * belongs to. Maps the L2-canonical "store opened by player" intent into the
 * standard exchange terminology consumers expect for charting and aggregation.
 *
 * <p>Distinct from
 * {@link app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore} —
 * that enum describes a character's current in-world state (which kind of
 * store window the player has open, including {@code CRAFT} and
 * {@code PACKAGE_SELL}). This enum describes an order's role on the order
 * book; only the two sides that produce price-discovery data are modeled.</p>
 *
 * <p>For purchases on the wire ({@link PrivateStorePurchaseEvent#getStoreType()}):
 * indicates which party opened the store that the deal closed in —
 * {@link #ASK} means the seller opened a SELL store and a buyer hit it,
 * {@link #BID} means the buyer opened a BUY store and a seller hit it. This
 * is the maker/taker direction signal for downstream analytics.</p>
 */
public enum PrivateStoreSide {

    /**
     * Ask side — open offer to SELL an item. Originates from a player's
     * private-store SELL window. In a closed trade, the store-opener is the
     * seller; the counterparty (buyer) is the taker.
     */
    ASK,

    /**
     * Bid side — open offer to BUY an item. Originates from a player's
     * private-store BUY window. In a closed trade, the store-opener is the
     * buyer; the counterparty (seller) is the taker.
     */
    BID
}
