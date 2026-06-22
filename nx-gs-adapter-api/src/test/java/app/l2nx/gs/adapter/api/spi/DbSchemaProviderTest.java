package app.l2nx.gs.adapter.api.spi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Compile-smoke for the Tier-2 SPI contract: an anonymous-class implementation
 * wires up cleanly and round-trips the values it declares. Guards against
 * accidental method-signature changes.
 */
class DbSchemaProviderTest {

    @Test
    void provider_shouldExposeNameAndMappings() {
        final EntityMapping<Object> clan = mapping("clan");

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

    private static EntityMapping<Object> mapping(final String entity) {
        final PrimarySource<Object> primary = new PrimarySource<Object>() {
            @Override
            public String tableName() {
                return entity + "_data";
            }

            @Override
            public String pkColumn() {
                return entity + "_id";
            }

            @Override
            public List<String> hashedColumns() {
                return java.util.Arrays.asList("col_a", "col_b");
            }

            @Override
            public Object mapRow(ResultSet rs) {
                return new Object();
            }
        };
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return entity;
            }

            @Override
            public Class<Object> dtoType() {
                return Object.class;
            }

            @Override
            public PrimarySource<?> primary() {
                return primary;
            }

            @Override
            public List<ChildSource<?>> children() {
                return Collections.emptyList();
            }

            @Override
            public Object mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                return primaryRow;
            }
        };
    }
}
