package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase;

/**
 * Canonical service codes used as the {@link PurchaseService#getCode() code}
 * value of a {@code PurchaseService} line. Hosts MAY use additional
 * non-canonical codes — the platform treats unknown codes as opaque strings —
 * but populating one of these constants when applicable lets cross-tenant
 * dashboards aggregate the same service consistently.
 *
 * <p>Catalog curated from L2 Lineage 2 game-mechanic vocabulary covering
 * L2J / Lucera / Essence forks. Adding a new constant is a non-breaking
 * minor-version change in {@code nx-gs-adapter-api}.</p>
 */
public final class WellKnownServices {

    private WellKnownServices() {
    }

    /**
     * Grant noblesse status.
     */
    public static final String NOBLESSE = "noblesse";

    /**
     * Activate / unlock a subclass slot.
     */
    public static final String SUBCLASS = "subclass";

    /**
     * Change character gender.
     */
    public static final String SEX_CHANGE = "sex_change";

    /**
     * Rename character. {@code params}: {@code old}, {@code new}.
     */
    public static final String NAME_CHANGE = "name_change";

    /**
     * Change character name color. {@code params}: {@code rgb} (hex string,
     * e.g. {@code "0xFFCC00"}).
     */
    public static final String NAME_COLOR_CHANGE = "name_color_change";

    /**
     * Change character title color. {@code params}: {@code rgb}.
     */
    public static final String TITLE_COLOR_CHANGE = "title_color_change";

    /**
     * Grant time-limited hero status. {@code params}: {@code minutes},
     * optional {@code skills} ({@code "true"}/{@code "false"}).
     */
    public static final String HERO_TEMPORARY = "hero_temporary";

    /**
     * Increment clan level by one.
     */
    public static final String CLAN_LVL_UP = "clan_lvl_up";

    /**
     * Buy a clan skill. {@code params}: {@code skill_id}, {@code level}.
     */
    public static final String CLAN_SKILL_BUY = "clan_skill_buy";

    /**
     * Buy clan reputation points. {@code params}: {@code count}.
     */
    public static final String CLAN_REP_BUY = "clan_rep_buy";

    /**
     * Buy clan fame. {@code params}: {@code count}.
     */
    public static final String CLAN_FAME_BUY = "clan_fame_buy";

    /**
     * Remove the clan-creation cooldown penalty.
     */
    public static final String CLAN_CREATE_PENALTY_REMOVE = "clan_create_penalty_remove";

    /**
     * Remove the post-clan-join cooldown penalty.
     */
    public static final String CLAN_JOIN_PENALTY_REMOVE = "clan_join_penalty_remove";

    /**
     * Remove the clan-invite cooldown penalty.
     */
    public static final String CLAN_INVITE_PENALTY_REMOVE = "clan_invite_penalty_remove";

    /**
     * Remove the alliance-related cooldown penalty.
     */
    public static final String ALLY_PENALTY_REMOVE = "ally_penalty_remove";

    /**
     * Add levels to the character. {@code params}: {@code count}.
     */
    public static final String LEVEL_UP = "level_up";

    /**
     * Subtract levels from the character. {@code params}: {@code count}.
     */
    public static final String LEVEL_DOWN = "level_down";

    /**
     * Reset karma meter to zero.
     */
    public static final String KARMA_RECOVER = "karma_recover";

    /**
     * Clear the player-killer flag.
     */
    public static final String PK_RECOVER = "pk_recover";

    /**
     * Restore vitality / stamina to full.
     */
    public static final String VITALITY_RECOVER = "vitality_recover";

    /**
     * Add or re-roll an augmentation on equipment. {@code params}:
     * {@code item_obj_id}, optional {@code grade} ({@code "white"}/
     * {@code "blue"}/{@code "purple"}).
     */
    public static final String AUGMENTATION = "augmentation";

    /**
     * Buy olympiad points. {@code params}: {@code count}.
     */
    public static final String OLYMPIAD_PTS_BUY = "olympiad_pts_buy";

    /**
     * Transfer a soul cloak between characters on the same account.
     * {@code params}: {@code from_char_id}, {@code to_char_id}.
     */
    public static final String SOUL_CLOAK_TRANSFER = "soul_cloak_transfer";
}
