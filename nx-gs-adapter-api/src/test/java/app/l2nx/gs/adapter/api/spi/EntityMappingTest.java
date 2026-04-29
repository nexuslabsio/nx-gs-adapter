package app.l2nx.gs.adapter.api.spi;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Compile-smoke for the Tier-2 SPI contract: an anonymous-class implementation
 * declares all required surface and round-trips the values it returns.
 */
class EntityMappingTest {

    @Test
    void mapping_shouldExposeAllFields() {
        EntityMapping<String> clan = new EntityMapping<String>() {
            @Override
            public String entityName() {
                return "clan";
            }

            @Override
            public String tableName() {
                return "clan_data";
            }

            @Override
            public String pkColumn() {
                return "clan_id";
            }

            @Override
            public List<String> hashedColumns() {
                return Arrays.asList("clan_name", "clan_level");
            }

            @Override
            public String mapRow(ResultSet rs) throws SQLException {
                return rs.getString("clan_name");
            }

            @Override
            public Class<String> dtoType() {
                return String.class;
            }
        };

        assertEquals("clan", clan.entityName());
        assertEquals("clan_data", clan.tableName());
        assertEquals("clan_id", clan.pkColumn());
        assertEquals(Arrays.asList("clan_name", "clan_level"), clan.hashedColumns());
        assertSame(String.class, clan.dtoType());
    }
}
