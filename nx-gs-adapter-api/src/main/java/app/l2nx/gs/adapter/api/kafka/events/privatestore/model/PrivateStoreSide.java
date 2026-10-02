package app.l2nx.gs.adapter.api.kafka.events.privatestore.model;

import app.l2nx.gs.adapter.api.kafka.events.privatestore.PrivateStorePurchaseEvent;

/**
 * Side of the private-store order book for an offer or closed trade.
 * <p>Distinct from {@link app.l2nx.gs.adapter.api.domain.character.CharacterPrivateStore}, which is a character's current in-world store state (incl. {@code CRAFT} / {@code PACKAGE_SELL}); only the two price-discovery sides are modeled here.
 * <p>On {@link PrivateStorePurchaseEvent#getStoreType()}: {@link #ASK} = seller opened a SELL store and a buyer hit it; {@link #BID} = buyer opened a BUY store and a seller hit it (maker/taker signal).
 */
public enum PrivateStoreSide {
    ASK,

    BID
}
