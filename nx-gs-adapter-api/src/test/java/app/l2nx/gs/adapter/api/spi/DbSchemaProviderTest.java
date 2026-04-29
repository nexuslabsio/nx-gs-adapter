package app.l2nx.gs.adapter.api.spi;

import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Compile-smoke for the Tier-2 SPI contract: an anonymous-class implementation
 * wires up cleanly and round-trips the values it declares. Guards against
 * accidental method-signature changes.
 */
class DbSchemaProviderTest {

    @Test
    void provider_shouldExposeNameAndMappings() {
        EntityMapping<Object> clan = mapping("clan", "clan_data", "clan_id");

        DbSchemaProvider provider = new DbSchemaProvider() {
            @Override
            public String schemaName() {
                return "stub";
            }

            @Override
            public List<EntityMapping<?>> mappings() {
                return Collections.singletonList(clan);
            }
        };

        assertEquals("stub", provider.schemaName());
        assertSame(clan, provider.mappings().get(0));
    }

    private static EntityMapping<Object> mapping(final String entity,
                                                 final String table,
                                                 final String pk) {
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return entity;
            }

            @Override
            public String tableName() {
                return table;
            }

            @Override
            public String pkColumn() {
                return pk;
            }

            @Override
            public List<String> hashedColumns() {
                return Arrays.asList("col_a", "col_b");
            }

            @Override
            public Object mapRow(ResultSet rs) {
                return new Object();
            }

            @Override
            public Class<Object> dtoType() {
                return Object.class;
            }
        };
    }
}
