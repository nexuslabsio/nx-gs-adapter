package app.l2nx.gs.db.sync;

import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.ParentRef;
import app.l2nx.gs.adapter.api.spi.PrimarySource;
import org.junit.jupiter.api.Test;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DbSyncModuleIdentValidationTest {

    @Test
    void validateIdentifiers_shouldAcceptCleanMapping() {
        DbSyncModule.validateIdentifiers(
                Collections.singletonList(mapping("clan_data", "clan_id",
                        Arrays.asList("clan_name", "clan_level"),
                        Collections.emptyList())));
    }

    @Test
    void validateIdentifiers_shouldRejectSqlInjectionInTableName() {
        EntityMapping<?> bad = mapping("clan_data; DROP TABLE x", "clan_id",
                Collections.singletonList("clan_name"), Collections.emptyList());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DbSyncModule.validateIdentifiers(Collections.singletonList(bad)));
        assertTrue(ex.getMessage().contains("primary.tableName"));
    }

    @Test
    void validateIdentifiers_shouldRejectSqlInjectionInPkColumn() {
        EntityMapping<?> bad = mapping("clan_data", "clan_id; DROP TABLE x",
                Collections.singletonList("clan_name"), Collections.emptyList());

        assertThrows(IllegalStateException.class,
                () -> DbSyncModule.validateIdentifiers(Collections.singletonList(bad)));
    }

    @Test
    void validateIdentifiers_shouldRejectSqlInjectionInHashedColumn() {
        EntityMapping<?> bad = mapping("clan_data", "clan_id",
                Arrays.asList("clan_name", "x; DROP TABLE clan_data; --"),
                Collections.emptyList());

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DbSyncModule.validateIdentifiers(Collections.singletonList(bad)));
        assertTrue(ex.getMessage().contains("hashedColumns"));
    }

    @Test
    void validateIdentifiers_shouldRejectSqlInjectionInChildSource() {
        ChildSource<Object> hostileChild = new ChildSource<Object>() {
            @Override
            public String tableName() {
                return "clan_skills";
            }

            @Override
            public String fkColumn() {
                return "clan_id; DROP TABLE x";
            }

            @Override
            public List<String> hashedColumns() {
                return Collections.singletonList("skill_id");
            }

            @Override
            public Object mapRow(ResultSet rs) {
                return null;
            }
        };
        EntityMapping<?> bad = mapping("clan_data", "clan_id",
                Collections.singletonList("clan_name"),
                Collections.singletonList(hostileChild));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DbSyncModule.validateIdentifiers(Collections.singletonList(bad)));
        assertTrue(ex.getMessage().contains("child.fkColumn"));
    }

    @Test
    void validateIdentifiers_shouldAcceptParentRefReferencingDeclaredEntity() {
        // mapping(...) declares entity "clan" — the parent the item refs point at.
        EntityMapping<?> clan = mapping("clan_data", "clan_id",
                Collections.singletonList("clan_name"), Collections.emptyList());
        EntityMapping<?> item = withParentRefs(
                mapping("items", "object_id", Collections.singletonList("count"),
                        Collections.emptyList()),
                "item",
                Collections.singletonList(ParentRef.of("clan", "owner_id")));

        DbSyncModule.validateIdentifiers(Arrays.asList(clan, item));
    }

    @Test
    void validateIdentifiers_shouldRejectParentRefWithUnknownParentEntity() {
        EntityMapping<?> item = withParentRefs(
                mapping("items", "object_id", Collections.singletonList("count"),
                        Collections.emptyList()),
                "item",
                Collections.singletonList(ParentRef.of("ghost", "owner_id")));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DbSyncModule.validateIdentifiers(Collections.singletonList(item)));
        assertTrue(ex.getMessage().contains("parentRef.parentEntityName"));
    }

    @Test
    void validateIdentifiers_shouldRejectSqlInjectionInParentRefFkColumn() {
        EntityMapping<?> clan = mapping("clan_data", "clan_id",
                Collections.singletonList("clan_name"), Collections.emptyList());
        EntityMapping<?> item = withParentRefs(
                mapping("items", "object_id", Collections.singletonList("count"),
                        Collections.emptyList()),
                "item",
                Collections.singletonList(ParentRef.of("clan", "owner_id; DROP TABLE x")));

        IllegalStateException ex = assertThrows(IllegalStateException.class,
                () -> DbSyncModule.validateIdentifiers(Arrays.asList(clan, item)));
        assertTrue(ex.getMessage().contains("parentRef.fkColumn"));
    }

    private static EntityMapping<Object> withParentRefs(final EntityMapping<Object> delegate,
                                                        final String entityName,
                                                        final List<ParentRef> refs) {
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return entityName;
            }

            @Override
            public Class<Object> dtoType() {
                return delegate.dtoType();
            }

            @Override
            public PrimarySource<?> primary() {
                return delegate.primary();
            }

            @Override
            public List<ChildSource<?>> children() {
                return delegate.children();
            }

            @Override
            public List<ParentRef> parentRefs() {
                return refs;
            }

            @Override
            public Object mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                return delegate.mapEntity(primaryRow, childRowsByTable);
            }
        };
    }

    private static EntityMapping<Object> mapping(final String table, final String pk,
                                                 final List<String> hashed,
                                                 final List<ChildSource<?>> children) {
        final PrimarySource<Object> primary = new PrimarySource<Object>() {
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
                return hashed;
            }

            @Override
            public Object mapRow(ResultSet rs) {
                return null;
            }
        };
        return new EntityMapping<Object>() {
            @Override
            public String entityName() {
                return "clan";
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
                return children;
            }

            @Override
            public Object mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                return primaryRow;
            }
        };
    }
}
