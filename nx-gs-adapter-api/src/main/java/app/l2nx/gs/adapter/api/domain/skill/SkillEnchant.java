package app.l2nx.gs.adapter.api.domain.skill;

/** Both values are {@code > 0}; a non-enchanted skill is a {@code null} {@code SkillEnchant} on the owner, never a {@code 0} / {@code 0} sentinel. */
public final class SkillEnchant {

    private final int route;
    private final int level;

    public SkillEnchant(int route, int level) {
        if (route <= 0) {
            throw new IllegalArgumentException("route must be > 0, got " + route);
        }
        if (level <= 0) {
            throw new IllegalArgumentException("level must be > 0, got " + level);
        }
        this.route = route;
        this.level = level;
    }

    public int getRoute() {
        return route;
    }

    public int getLevel() {
        return level;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SkillEnchant)) return false;
        SkillEnchant that = (SkillEnchant) o;
        return route == that.route && level == that.level;
    }

    @Override
    public int hashCode() {
        return 31 * route + level;
    }

    @Override
    public String toString() {
        return "SkillEnchant[route=" + route + ", level=" + level + "]";
    }
}
