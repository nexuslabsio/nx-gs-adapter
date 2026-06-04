package app.l2nx.gs.adapter.api.domain.item;

/**
 * Top-level item category — the universal L2 weapongrp / armorgrp / etcitemgrp
 * trichotomy. A shared item-domain enum (not tied to one wire DTO). Closed and
 * build-agnostic: every L2 item structurally belongs to exactly one of these, so a
 * provider can always classify (non-null on the wire).
 */
public enum ItemClass {
    WEAPON,
    ARMOR,
    ETC
}
