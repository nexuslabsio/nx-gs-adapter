package app.l2nx.gs.adapter.api.kafka.events.premiumpurchase.model;

/**
 * Canonical codes for {@link PurchaseService#getCode()}; hosts MAY use others (opaque to the platform), but these let cross-tenant dashboards aggregate consistently.
 * <p>Adding a constant is a non-breaking minor-version change in {@code nx-gs-adapter-api}.
 */
public final class WellKnownServices {

    private WellKnownServices() {}

    public static final String NOBLESSE = "noblesse";

    public static final String SUBCLASS = "subclass";

    public static final String SEX_CHANGE = "sex_change";

    /** {@code params}: {@code old}, {@code new}. */
    public static final String NAME_CHANGE = "name_change";

    /** {@code params}: {@code rgb} (hex string, e.g. {@code "0xFFCC00"}). */
    public static final String NAME_COLOR_CHANGE = "name_color_change";

    /** {@code params}: {@code rgb}. */
    public static final String TITLE_COLOR_CHANGE = "title_color_change";

    /** {@code params}: {@code minutes}, optional {@code skills} ({@code "true"}/{@code "false"}). */
    public static final String HERO_TEMPORARY = "hero_temporary";

    public static final String CLAN_LVL_UP = "clan_lvl_up";

    /** {@code params}: {@code skill_id}, {@code level}. */
    public static final String CLAN_SKILL_BUY = "clan_skill_buy";

    /** {@code params}: {@code count}. */
    public static final String CLAN_REP_BUY = "clan_rep_buy";

    /** {@code params}: {@code count}. */
    public static final String CLAN_FAME_BUY = "clan_fame_buy";

    public static final String CLAN_CREATE_PENALTY_REMOVE = "clan_create_penalty_remove";

    public static final String CLAN_JOIN_PENALTY_REMOVE = "clan_join_penalty_remove";

    public static final String CLAN_INVITE_PENALTY_REMOVE = "clan_invite_penalty_remove";

    public static final String ALLY_PENALTY_REMOVE = "ally_penalty_remove";

    /** {@code params}: {@code count}. */
    public static final String LEVEL_UP = "level_up";

    /** {@code params}: {@code count}. */
    public static final String LEVEL_DOWN = "level_down";

    public static final String KARMA_RECOVER = "karma_recover";

    public static final String PK_RECOVER = "pk_recover";

    public static final String VITALITY_RECOVER = "vitality_recover";

    /** {@code params}: {@code item_obj_id}, optional {@code grade} ({@code "white"}/{@code "blue"}/{@code "purple"}). */
    public static final String AUGMENTATION = "augmentation";

    /** {@code params}: {@code count}. */
    public static final String OLYMPIAD_PTS_BUY = "olympiad_pts_buy";

    /** {@code params}: {@code from_char_id}, {@code to_char_id}. */
    public static final String SOUL_CLOAK_TRANSFER = "soul_cloak_transfer";

    public static final String PREMIUM_ACCOUNT_1H = "premium_account_1h";

    public static final String PREMIUM_ACCOUNT_4H = "premium_account_4h";

    public static final String PREMIUM_ACCOUNT_1D = "premium_account_1d";

    public static final String PREMIUM_ACCOUNT_7D = "premium_account_7d";

    public static final String PREMIUM_ACCOUNT_30D = "premium_account_30d";
}
