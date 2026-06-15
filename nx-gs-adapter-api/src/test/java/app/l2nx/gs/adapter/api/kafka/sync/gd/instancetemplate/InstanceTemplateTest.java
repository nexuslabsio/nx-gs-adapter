package app.l2nx.gs.adapter.api.kafka.sync.gd.instancetemplate;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class InstanceTemplateTest {

    private static LocalizedText name() {
        Map<String, String> values = new LinkedHashMap<String, String>();
        values.put("en", "Antharas Lair");
        values.put("ru", "Логово Антараса");
        return new LocalizedText(values);
    }

    @Test
    void builder_shouldMatchConstructor() {
        InstanceTemplate fromBuilder = InstanceTemplate.builder().id(112).name(name()).build();
        InstanceTemplate fromCtor = new InstanceTemplate(112, name());

        assertEquals(fromCtor, fromBuilder);
        assertEquals(fromCtor.hashCode(), fromBuilder.hashCode());
    }

    @Test
    void name_shouldBeNullable() {
        InstanceTemplate noName = InstanceTemplate.builder().id(5).build();

        assertNull(noName.getName());
        assertEquals(5, noName.getId());
    }

    @Test
    void toBuilder_shouldRoundtrip() {
        InstanceTemplate original = new InstanceTemplate(9, name());

        assertEquals(original, original.toBuilder().build());
    }
}
