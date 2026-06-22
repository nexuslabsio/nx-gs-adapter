package app.l2nx.gs.adapter.api.spi;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Compile-smoke for the Tier-2 SPI contract: an anonymous-class implementation
 * declares all required surface and round-trips the values it returns.
 */
class EntityMappingTest {

    @Test
    void mapping_shouldExposeEntityIdentityAndPrimary() {
        final PrimarySource<String> primary = new PrimarySource<String>() {
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
        };
        EntityMapping<String> mapping = new EntityMapping<String>() {
            @Override
            public String entityName() {
                return "clan";
            }

            @Override
            public Class<String> dtoType() {
                return String.class;
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
            public String mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                return (String) primaryRow;
            }
        };

        assertEquals("clan", mapping.entityName());
        assertSame(String.class, mapping.dtoType());
        assertSame(primary, mapping.primary());
        assertTrue(mapping.children().isEmpty());
    }

    @Test
    void mapping_shouldFoldChildren_intoMapEntityKeyedByTableName() {
        final ChildSource<String> skills = new ChildSource<String>() {
            @Override
            public String tableName() {
                return "clan_skills";
            }

            @Override
            public String fkColumn() {
                return "clan_id";
            }

            @Override
            public List<String> hashedColumns() {
                return Arrays.asList("skill_id", "skill_level");
            }

            @Override
            public String mapRow(ResultSet rs) throws SQLException {
                return rs.getString("skill_id") + ":" + rs.getString("skill_level");
            }
        };
        EntityMapping<String> mapping = new EntityMapping<String>() {
            @Override
            public String entityName() {
                return "clan";
            }

            @Override
            public Class<String> dtoType() {
                return String.class;
            }

            @Override
            public PrimarySource<?> primary() {
                return primaryStub();
            }

            @Override
            public List<ChildSource<?>> children() {
                return Collections.singletonList(skills);
            }

            @Override
            public String mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                List<Object> rows = childRowsByTable.get("clan_skills");
                return primaryRow + "|" + (rows == null ? "0" : Integer.toString(rows.size()));
            }
        };

        assertEquals(1, mapping.children().size());
        assertSame(skills, mapping.children().get(0));
        // mapEntity round-trip with two child rows
        Map<String, List<Object>> children = Collections.singletonMap("clan_skills", Arrays.asList("a", "b"));
        assertEquals("primary|2", mapping.mapEntity("primary", children));
        assertNotNull(mapping.children());
    }

    @Test
    void parentRefs_shouldDefaultToEmpty() {
        EntityMapping<String> mapping = new EntityMapping<String>() {
            @Override
            public String entityName() {
                return "item";
            }

            @Override
            public Class<String> dtoType() {
                return String.class;
            }

            @Override
            public PrimarySource<?> primary() {
                return primaryStub();
            }

            @Override
            public List<ChildSource<?>> children() {
                return Collections.emptyList();
            }

            @Override
            public String mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                return (String) primaryRow;
            }
        };

        assertNotNull(mapping.parentRefs());
        assertTrue(mapping.parentRefs().isEmpty());
    }

    private static PrimarySource<String> primaryStub() {
        return new PrimarySource<String>() {
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
                return Collections.singletonList("clan_name");
            }

            @Override
            public String mapRow(ResultSet rs) {
                return null;
            }
        };
    }
}
