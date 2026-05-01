package app.l2nx.gs.adapter.api.domain.character;

import org.jspecify.annotations.Nullable;

/**
 * Player character class — canonical Salvation/High-Five class set surfaced
 * on the wire. 103 entries.
 *
 * <p>The enum carries class identity only: a source-side numeric ID.
 * Per-class semantics (tier / race / mage / summoner / equipment rules /
 * stat tables) are tenant- and chronicle-specific and live in tenant
 * settings on the platform side, not in this contract.</p>
 *
 * <p>Naming convention: race prefix only on overlapping base names
 * ({@code FIGHTER}, {@code MAGE}, {@code SOLDIER}). Higher-tier class
 * names are unique enough to stand alone, except where the L2 source
 * itself disambiguates (e.g. {@code ELVEN_KNIGHT} vs {@code KNIGHT},
 * {@code DARK_WIZARD} vs {@code WIZARD}, {@code SHILLIEN_ORACLE} vs
 * {@code ORACLE}). Kamael gendered classes keep the gender prefix
 * ({@code MALE_SOULBREAKER}, {@code FEMALE_SOULHOUND}) but no race prefix —
 * the class names are unique to Kamael.</p>
 *
 * <p>Schema providers translate the source-side numeric class ID via a
 * switch into this enum in their {@code PrimarySource.mapRow()}
 * implementation. Source IDs not present here (Awakened-only classes,
 * Sylph, dummy slots, future chronicles) are logged as warnings by the
 * schema provider and surface as {@code null} on the wire.</p>
 */
public enum CharacterClass {

    // BASIC — newborn, no profession yet
    HUMAN_FIGHTER(0),
    HUMAN_MAGE(10),
    ELVEN_FIGHTER(18),
    ELVEN_MAGE(25),
    DARK_FIGHTER(31),
    DARK_MAGE(38),
    ORC_FIGHTER(44),
    ORC_MAGE(49),
    DWARVEN_FIGHTER(53),
    KAMAEL_MALE_SOLDIER(123),
    KAMAEL_FEMALE_SOLDIER(124),

    // FIRST profession
    WARRIOR(1),
    KNIGHT(4),
    ROGUE(7),
    WIZARD(11),
    CLERIC(15),
    ELVEN_KNIGHT(19),
    ELVEN_SCOUT(22),
    ELVEN_WIZARD(26),
    ORACLE(29),
    PALUS_KNIGHT(32),
    ASSASSIN(35),
    DARK_WIZARD(39),
    SHILLIEN_ORACLE(42),
    ORC_RAIDER(45),
    ORC_MONK(47),
    ORC_SHAMAN(50),
    SCAVENGER(54),
    ARTISAN(56),
    TROOPER(125),
    WARDER(126),

    // SECOND profession
    GLADIATOR(2),
    WARLORD(3),
    PALADIN(5),
    DARK_AVENGER(6),
    TREASURE_HUNTER(8),
    HAWKEYE(9),
    SORCERER(12),
    NECROMANCER(13),
    WARLOCK(14),
    BISHOP(16),
    PROPHET(17),
    TEMPLE_KNIGHT(20),
    SWORDSINGER(21),
    PLAINS_WALKER(23),
    SILVER_RANGER(24),
    SPELLSINGER(27),
    ELEMENTAL_SUMMONER(28),
    ELDER(30),
    SHILLIEN_KNIGHT(33),
    BLADEDANCER(34),
    ABYSS_WALKER(36),
    PHANTOM_RANGER(37),
    SPELLHOWLER(40),
    PHANTOM_SUMMONER(41),
    SHILLIEN_ELDER(43),
    DESTROYER(46),
    TYRANT(48),
    OVERLORD(51),
    WARCRYER(52),
    BOUNTY_HUNTER(55),
    WARSMITH(57),
    BERSERKER(127),
    MALE_SOULBREAKER(128),
    FEMALE_SOULBREAKER(129),
    ARBALESTER(130),
    INSPECTOR(135),

    // THIRD profession
    DUELIST(88),
    DREADNOUGHT(89),
    PHOENIX_KNIGHT(90),
    HELL_KNIGHT(91),
    SAGITTARIUS(92),
    ADVENTURER(93),
    ARCHMAGE(94),
    SOULTAKER(95),
    ARCANA_LORD(96),
    CARDINAL(97),
    HIEROPHANT(98),
    EVA_TEMPLAR(99),
    SWORD_MUSE(100),
    WIND_RIDER(101),
    MOONLIGHT_SENTINEL(102),
    MYSTIC_MUSE(103),
    ELEMENTAL_MASTER(104),
    EVA_SAINT(105),
    SHILLIEN_TEMPLAR(106),
    SPECTRAL_DANCER(107),
    GHOST_HUNTER(108),
    GHOST_SENTINEL(109),
    STORM_SCREAMER(110),
    SPECTRAL_MASTER(111),
    SHILLIEN_SAINT(112),
    TITAN(113),
    GRAND_KHAVATARI(114),
    DOMINATOR(115),
    DOOMCRYER(116),
    FORTUNE_SEEKER(117),
    MAESTRO(118),
    DOOMBRINGER(131),
    MALE_SOULHOUND(132),
    FEMALE_SOULHOUND(133),
    TRICKSTER(134),
    JUDICATOR(136);

    private static final CharacterClass[] BY_ID;

    static {
        int max = 0;
        for (CharacterClass c : values()) {
            if (c.id > max) max = c.id;
        }
        CharacterClass[] table = new CharacterClass[max + 1];
        for (CharacterClass c : values()) {
            table[c.id] = c;
        }
        BY_ID = table;
    }

    private final int id;

    CharacterClass(int id) {
        this.id = id;
    }

    /**
     * Source-side numeric class ID.
     */
    public int getId() {
        return id;
    }

    /**
     * Resolves a numeric class ID into the enum constant.
     *
     * @param id source-side numeric class ID
     * @return the matching constant, or {@code null} when the ID is not
     * in this enum's canonical Salvation/High-Five set
     */
    public static @Nullable CharacterClass byId(int id) {
        if (id < 0 || id >= BY_ID.length) return null;
        return BY_ID[id];
    }
}
