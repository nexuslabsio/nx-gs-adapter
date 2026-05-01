package app.l2nx.gs.adapter.api.domain.character;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CharacterPrivateStoreTest {

    @Test
    void byId_shouldRoundTrip_forEveryConstant() {
        for (CharacterPrivateStore m : CharacterPrivateStore.values()) {
            assertSame(m, CharacterPrivateStore.byId(m.getId()));
        }
    }

    @Test
    void byId_shouldReturnNull_forNonSurfacedIds() {
        // 0 = no store, 2 = sell pending (menu), 4 = buy pending (menu), 6/7 = manage modes.
        assertNull(CharacterPrivateStore.byId(0));
        assertNull(CharacterPrivateStore.byId(2));
        assertNull(CharacterPrivateStore.byId(4));
        assertNull(CharacterPrivateStore.byId(6));
        assertNull(CharacterPrivateStore.byId(7));
        assertNull(CharacterPrivateStore.byId(-1));
        assertNull(CharacterPrivateStore.byId(99));
    }

    @Test
    void canonicalSet_shouldHave4Entries() {
        assertEquals(4, CharacterPrivateStore.values().length);
    }

    @Test
    void canonicalSamples_shouldResolveToExpectedConstant() {
        assertSame(CharacterPrivateStore.SELL, CharacterPrivateStore.byId(1));
        assertSame(CharacterPrivateStore.BUY, CharacterPrivateStore.byId(3));
        assertSame(CharacterPrivateStore.CRAFT, CharacterPrivateStore.byId(5));
        assertSame(CharacterPrivateStore.PACKAGE_SELL, CharacterPrivateStore.byId(8));
    }
}
