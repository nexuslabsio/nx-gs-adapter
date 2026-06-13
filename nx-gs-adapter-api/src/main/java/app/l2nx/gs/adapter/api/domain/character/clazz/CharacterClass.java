package app.l2nx.gs.adapter.api.domain.character.clazz;

/**
 * Player character class — canonical Salvation/High-Five class set surfaced on the wire.
 * 103 entries. A pure, source-agnostic vocabulary: the enum carries class identity only, by
 * canonical token. The source-side numeric class id is deliberately NOT modeled here — that
 * mapping is a host detail and lives in the host provider; display names / translations are
 * platform data and live consumer-side (keyed by the token).
 *
 * <p>Naming convention: race prefix only on overlapping base names ({@code FIGHTER},
 * {@code MAGE}, {@code SOLDIER}). Higher-tier class names are unique enough to stand alone,
 * except where the L2 source itself disambiguates (e.g. {@code ELVEN_KNIGHT} vs {@code KNIGHT},
 * {@code DARK_WIZARD} vs {@code WIZARD}, {@code SHILLIEN_ORACLE} vs {@code ORACLE}). Kamael
 * gendered classes keep the gender prefix ({@code MALE_SOULBREAKER}, {@code FEMALE_SOULHOUND}).
 *
 * <p>A non-canonical / custom-fork class is a contract gap: add the constant here (and to the
 * host id→enum mapping) rather than surfacing it as {@code null}.
 */
public enum CharacterClass {

    // BASIC — newborn, no profession yet
    HUMAN_FIGHTER,
    HUMAN_MAGE,
    ELVEN_FIGHTER,
    ELVEN_MAGE,
    DARK_FIGHTER,
    DARK_MAGE,
    ORC_FIGHTER,
    ORC_MAGE,
    DWARVEN_FIGHTER,
    KAMAEL_MALE_SOLDIER,
    KAMAEL_FEMALE_SOLDIER,

    // FIRST profession
    WARRIOR,
    KNIGHT,
    ROGUE,
    WIZARD,
    CLERIC,
    ELVEN_KNIGHT,
    ELVEN_SCOUT,
    ELVEN_WIZARD,
    ORACLE,
    PALUS_KNIGHT,
    ASSASSIN,
    DARK_WIZARD,
    SHILLIEN_ORACLE,
    ORC_RAIDER,
    ORC_MONK,
    ORC_SHAMAN,
    SCAVENGER,
    ARTISAN,
    TROOPER,
    WARDER,

    // SECOND profession
    GLADIATOR,
    WARLORD,
    PALADIN,
    DARK_AVENGER,
    TREASURE_HUNTER,
    HAWKEYE,
    SORCERER,
    NECROMANCER,
    WARLOCK,
    BISHOP,
    PROPHET,
    TEMPLE_KNIGHT,
    SWORDSINGER,
    PLAINS_WALKER,
    SILVER_RANGER,
    SPELLSINGER,
    ELEMENTAL_SUMMONER,
    ELDER,
    SHILLIEN_KNIGHT,
    BLADEDANCER,
    ABYSS_WALKER,
    PHANTOM_RANGER,
    SPELLHOWLER,
    PHANTOM_SUMMONER,
    SHILLIEN_ELDER,
    DESTROYER,
    TYRANT,
    OVERLORD,
    WARCRYER,
    BOUNTY_HUNTER,
    WARSMITH,
    BERSERKER,
    MALE_SOULBREAKER,
    FEMALE_SOULBREAKER,
    ARBALESTER,
    INSPECTOR,

    // THIRD profession
    DUELIST,
    DREADNOUGHT,
    PHOENIX_KNIGHT,
    HELL_KNIGHT,
    SAGITTARIUS,
    ADVENTURER,
    ARCHMAGE,
    SOULTAKER,
    ARCANA_LORD,
    CARDINAL,
    HIEROPHANT,
    EVA_TEMPLAR,
    SWORD_MUSE,
    WIND_RIDER,
    MOONLIGHT_SENTINEL,
    MYSTIC_MUSE,
    ELEMENTAL_MASTER,
    EVA_SAINT,
    SHILLIEN_TEMPLAR,
    SPECTRAL_DANCER,
    GHOST_HUNTER,
    GHOST_SENTINEL,
    STORM_SCREAMER,
    SPECTRAL_MASTER,
    SHILLIEN_SAINT,
    TITAN,
    GRAND_KHAVATARI,
    DOMINATOR,
    DOOMCRYER,
    FORTUNE_SEEKER,
    MAESTRO,
    DOOMBRINGER,
    MALE_SOULHOUND,
    FEMALE_SOULHOUND,
    TRICKSTER,
    JUDICATOR
}
