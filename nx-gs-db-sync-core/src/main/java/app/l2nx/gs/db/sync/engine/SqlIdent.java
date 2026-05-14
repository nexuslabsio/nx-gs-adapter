package app.l2nx.gs.db.sync.engine;

import java.util.regex.Pattern;

/**
 * Validates provider-supplied SQL identifiers (table / column names) used to
 * construct CRC and fetch queries. Provider input is interpolated into SQL
 * without quoting; without validation a hostile or buggy provider could inject
 * arbitrary SQL.
 */
public final class SqlIdent {

    private static final Pattern VALID = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]{0,63}$");

    private SqlIdent() {
    }

    public static void validate(String name, String role) {
        if (name == null || !VALID.matcher(name).matches()) {
            throw new IllegalStateException(
                    "Identifier '" + name + "' for " + role
                            + " contains invalid characters; must match [A-Za-z_][A-Za-z0-9_]{0,63}");
        }
    }
}
