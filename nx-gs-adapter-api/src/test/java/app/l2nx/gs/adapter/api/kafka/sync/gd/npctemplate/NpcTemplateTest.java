package app.l2nx.gs.adapter.api.kafka.sync.gd.npctemplate;

import app.l2nx.gs.adapter.api.domain.npc.NpcRace;
import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Guards the 37-parameter constructor/builder ordering: with runs of adjacent same-typed
 * parameters a silent swap compiles fine — a full round-trip catches it.
 */
class NpcTemplateTest {

    @Test
    void toBuilder_shouldRoundTripEveryField() {
        NpcTemplate original = fullyPopulated();
        NpcTemplate copy = original.toBuilder().build();

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
    }

    @Test
    void builder_shouldPlaceEveryValueOnItsField() {
        NpcTemplate t = fullyPopulated();

        assertEquals(25501, t.getId());
        assertEquals("RAID_BOSS", t.getType());
        assertEquals(Integer.valueOf(25001), t.getDisplayId());
        assertEquals(Integer.valueOf(30), t.getLevel());
        assertEquals(NpcRace.BEAST, t.getRace());
        assertEquals("FIGHTER", t.getAiType());
        assertEquals("SOUL", t.getShots());
        assertEquals(Boolean.FALSE, t.getRandomMinions());
        assertEquals(Boolean.TRUE, t.getLethalImmune());
        assertEquals(Boolean.TRUE, t.getChampionDisabled());
        assertEquals(Boolean.TRUE, t.getNoRandomWalk());
        assertEquals(Boolean.TRUE, t.getMovementDisabled());
        assertEquals(Integer.valueOf(2000), t.getMaxPursueRange());
        assertEquals(Boolean.TRUE, t.getCanSeeInSilentMove());
        assertEquals(Boolean.TRUE, t.getGlobalAggro());
        assertEquals("skill4416_etc", t.getRaceIcon());
        assertEquals(Double.valueOf(45.5), t.getCollisionRadius());
        assertEquals(Double.valueOf(100.0), t.getCollisionHeight());
        assertEquals("BOW", t.getAtkType());
        assertEquals(Double.valueOf(53690.0), t.getStats().get("MAX_HP"));
        assertEquals(Long.valueOf(2308288L), t.getRewardExp());
        assertEquals(Long.valueOf(111484L), t.getRewardSp());
        assertEquals(Integer.valueOf(416), t.getRewardRp());
        assertEquals("orc_clan", t.getFaction().getName());
        assertEquals(Integer.valueOf(600), t.getFaction().getRange());
        assertEquals(Integer.valueOf(25502), t.getTransformOnDeadNpcTemplateId());
        assertEquals(Integer.valueOf(100), t.getTransformChancePercent());
        assertEquals(Integer.valueOf(3), t.getSpawnOnDeathCount());
        assertEquals(Integer.valueOf(40), t.getSpawnOnDeathChancePercent());
        assertEquals("Boss Akata", t.getName().values().get("en"));
        assertEquals("Raid Boss", t.getTitle().values().get("en"));
        assertEquals(Integer.valueOf(80), t.getRightHand());
        assertEquals(Integer.valueOf(641), t.getLeftHand());
        assertEquals(1, t.getSkills().size());
        assertEquals(1, t.getDrops().size());
        assertEquals(1, t.getMinions().size());
        assertEquals(1, t.getAbsorbs().size());
        assertEquals(1, t.getSpawns().size());
    }

    @Test
    void equals_shouldDetectSingleFieldDifference() {
        NpcTemplate a = fullyPopulated();
        NpcTemplate b = a.toBuilder().atkType("POLE").build();

        assertNotEquals(a, b);
    }

    @Test
    void stats_shouldBeDefensivelyCopiedAndUnmodifiable() {
        Map<String, Double> source = new LinkedHashMap<String, Double>();
        source.put("MAX_HP", 100.0);
        NpcTemplate t = NpcTemplate.builder().id(1).type("MONSTER").stats(source).build();

        source.put("MAX_MP", 50.0);
        assertEquals(1, t.getStats().size());
        assertThrows(UnsupportedOperationException.class, () -> t.getStats().put("P_ATK", 1.0));
    }

    private static NpcTemplate fullyPopulated() {
        Map<String, Double> stats = new LinkedHashMap<String, Double>();
        stats.put("MAX_HP", 53690.0);
        stats.put("AGGRO_RANGE", 500.0);
        Map<String, String> name = new LinkedHashMap<String, String>();
        name.put("en", "Boss Akata");
        Map<String, String> title = new LinkedHashMap<String, String>();
        title.put("en", "Raid Boss");
        return NpcTemplate.builder()
                .id(25501)
                .type("RAID_BOSS")
                .displayId(25001)
                .level(30)
                .race(NpcRace.BEAST)
                .aiType("FIGHTER")
                .shots("SOUL")
                .randomMinions(Boolean.FALSE)
                .lethalImmune(Boolean.TRUE)
                .championDisabled(Boolean.TRUE)
                .noRandomWalk(Boolean.TRUE)
                .movementDisabled(Boolean.TRUE)
                .maxPursueRange(2000)
                .canSeeInSilentMove(Boolean.TRUE)
                .globalAggro(Boolean.TRUE)
                .raceIcon("skill4416_etc")
                .collisionRadius(45.5)
                .collisionHeight(100.0)
                .atkType("BOW")
                .stats(stats)
                .rewardExp(2308288L)
                .rewardSp(111484L)
                .rewardRp(416)
                .faction(NpcFaction.builder().name("orc_clan").range(600).build())
                .transformOnDeadNpcTemplateId(25502)
                .transformChancePercent(100)
                .spawnOnDeathCount(3)
                .spawnOnDeathChancePercent(40)
                .name(LocalizedText.of(name))
                .title(LocalizedText.of(title))
                .rightHand(80)
                .leftHand(641)
                .skills(Collections.singletonList(NpcSkillRef.builder().id(4045).level(1).build()))
                .drops(Collections.singletonList(NpcDropGroup.builder()
                        .groupIndex(0)
                        .items(Collections.singletonList(
                                NpcDropItem.builder().itemTemplateId(57).build()))
                        .build()))
                .minions(Collections.singletonList(
                        NpcMinionRef.builder().minionNpcTemplateId(25502).count(2).build()))
                .absorbs(Collections.singletonList(
                        NpcAbsorb.builder().minLevel(13).maxLevel(13).skill(Boolean.TRUE).build()))
                .spawns(Collections.singletonList(
                        NpcSpawn.builder().x(1).y(2).z(3).heading(0).build()))
                .build();
    }
}
