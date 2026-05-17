package app.l2nx.gs.db.sync.engine;

import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanDbDto;
import app.l2nx.gs.adapter.api.kafka.sync.db.clan.ClanSkillDbDto;
import app.l2nx.gs.adapter.api.spi.ChildSource;
import app.l2nx.gs.adapter.api.spi.EntityMapping;
import app.l2nx.gs.adapter.api.spi.PrimarySource;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * Shared {@link EntityMapping} builders for engine tests. Each builder is
 * stateless and returns a fresh mapping per call; tests should not share
 * the same mapping across cycles unless they explicitly want to.
 */
public final class TestMappings {

    private TestMappings() {
    }

    /**
     * Single-table {@code clan_data} mapping — no children, mirrors the
     * pre-multi-source MVP shape. Used by tests that only need a typed
     * {@link EntityMapping<ClanDbDto>} surface and do not exercise child rows.
     */
    public static EntityMapping<ClanDbDto> clanOnly() {
        return clanWithChildren(Collections.emptyList());
    }

    /**
     * Clan mapping with the full bohpts shape: primary {@code clan_data}
     * (sentinel-zero → null for {@code leader_id} / {@code ally_id}) +
     * one child {@code clan_skills} (FK {@code clan_id}, hashed
     * {@code skill_id, skill_level}). {@code mapEntity} assembles a
     * {@link ClanDbDto} with a fully populated {@link ClanDbDto#getSkills() skills}
     * list.
     */
    public static EntityMapping<ClanDbDto> clanWithSkills() {
        ChildSource<TestClanSkillRow> skills = new ChildSource<TestClanSkillRow>() {
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
            public TestClanSkillRow mapRow(ResultSet rs) throws SQLException {
                return new TestClanSkillRow(rs.getInt("skill_id"), rs.getInt("skill_level"));
            }
        };
        return clanWithChildren(Collections.singletonList(skills));
    }

    private static EntityMapping<ClanDbDto> clanWithChildren(final List<ChildSource<?>> children) {
        final PrimarySource<TestClanRow> primary = new PrimarySource<TestClanRow>() {
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
                return Arrays.asList("clan_name", "clan_level", "leader_id", "ally_id");
            }

            @Override
            public TestClanRow mapRow(ResultSet rs) throws SQLException {
                return new TestClanRow(
                        rs.getLong("clan_id"),
                        rs.getString("clan_name"),
                        rs.getInt("clan_level"),
                        nullIfZero(rs.getLong("leader_id")),
                        nullIfZero(rs.getLong("ally_id")));
            }
        };
        return new EntityMapping<ClanDbDto>() {
            @Override
            public String entityName() {
                return "clan";
            }

            @Override
            public Class<ClanDbDto> dtoType() {
                return ClanDbDto.class;
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
            public ClanDbDto mapEntity(Object primaryRow, Map<String, List<Object>> childRowsByTable) {
                TestClanRow clan = (TestClanRow) primaryRow;
                List<Object> rawSkills = childRowsByTable.get("clan_skills");
                List<ClanSkillDbDto> skills;
                if (rawSkills == null || rawSkills.isEmpty()) {
                    skills = Collections.emptyList();
                } else {
                    skills = new ArrayList<ClanSkillDbDto>(rawSkills.size());
                    for (Object raw : rawSkills) {
                        TestClanSkillRow row = (TestClanSkillRow) raw;
                        skills.add(ClanSkillDbDto.builder()
                                .id(row.skillId)
                                .level(row.skillLevel)
                                .build());
                    }
                }
                return ClanDbDto.builder()
                        .id(clan.clanId)
                        .name(clan.clanName)
                        .level(clan.clanLevel)
                        .leaderId(clan.leaderId)
                        .allyId(clan.allyId)
                        .skills(skills)
                        .build();
            }
        };
    }

    /**
     * Generic stub mapping with a custom primary source (no children, no
     * Phase-2 row mapping). Good enough for tests that only exercise the
     * planner / hasher SQL surface.
     */
    public static EntityMapping<Object> stub(final String entity,
                                             final String table,
                                             final String pk,
                                             final List<String> hashed) {
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

    private static Long nullIfZero(long v) {
        return v == 0L ? null : v;
    }

    /**
     * Package-private primary-row record used by the clan mapping in tests.
     * Mirrors the bohpts impl's private {@code ClanRow}.
     */
    static final class TestClanRow {
        final long clanId;
        final String clanName;
        final int clanLevel;
        final Long leaderId;
        final Long allyId;

        TestClanRow(long clanId, String clanName, int clanLevel, Long leaderId, Long allyId) {
            this.clanId = clanId;
            this.clanName = clanName;
            this.clanLevel = clanLevel;
            this.leaderId = leaderId;
            this.allyId = allyId;
        }
    }

    static final class TestClanSkillRow {
        final int skillId;
        final int skillLevel;

        TestClanSkillRow(int skillId, int skillLevel) {
            this.skillId = skillId;
            this.skillLevel = skillLevel;
        }
    }
}
