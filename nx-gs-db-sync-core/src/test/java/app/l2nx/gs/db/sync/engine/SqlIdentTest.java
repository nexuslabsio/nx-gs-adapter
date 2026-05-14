package app.l2nx.gs.db.sync.engine;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlIdentTest {

    @Test
    void validate_shouldAcceptStandardSnakeCaseIdentifiers() {
        SqlIdent.validate("clan_data", "primary.tableName");
        SqlIdent.validate("clan_id", "primary.pkColumn");
        SqlIdent.validate("_underscore_start", "col");
        SqlIdent.validate("Col123", "col");
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "; DROP TABLE clan_data; --",
            "clan_data; DROP TABLE x",
            "clan_data --comment",
            "clan_data WHERE 1=1",
            "1startsWithDigit",
            "back`tick",
            "space inside",
            "dotted.name",
            "",
            " "
    })
    void validate_shouldRejectHostileOrMalformedIdentifiers(String name) {
        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> SqlIdent.validate(name, "primary.tableName"));
        assertTrue(ex.getMessage().contains("primary.tableName"));
        assertTrue(ex.getMessage().contains("must match"));
    }

    @Test
    void validate_shouldRejectNull() {
        assertThrows(IllegalStateException.class,
                () -> SqlIdent.validate(null, "primary.pkColumn"));
    }

    @Test
    void validate_shouldRejectIdentifierExceeding64Chars() {
        StringBuilder sb = new StringBuilder("a");
        for (int i = 0; i < 64; i++) {
            sb.append('x');
        }
        assertThrows(IllegalStateException.class,
                () -> SqlIdent.validate(sb.toString(), "col"));
    }
}
