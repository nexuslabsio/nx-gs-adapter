package app.l2nx.gs.adapter.api.kafka.sync.gd.instancetemplate;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import org.jspecify.annotations.Nullable;

import java.util.Objects;

/**
 * Build-agnostic instance-template wire DTO — the {@code id → name} catalog for
 * instanced zones (reflections), carried as the payload of
 * {@code GameDataSyncEvent} on the {@code gd} (game-data) sync stream's
 * {@code instance} entity topic. The host supplies a provider reading its
 * reflection-name catalog (e.g. {@code reflectionNames.xml}); nothing here
 * names a specific core.
 *
 * <p>Resolves the numeric {@code instanceId} carried per character on
 * {@code CharacterInstanceCooldownDbDto} into a readable, localized name — the
 * name is NOT denormalized onto every cooldown row.</p>
 *
 * <p>Only {@link #getId() id} is non-null. {@link #getName() name} is a
 * {@link LocalizedText} (locale → string, e.g. {@code {"en": ..., "ru": ...}}),
 * carried as-is on the wire; conversion to the platform {@code LocalizedText}
 * happens consumer-side in nx-gamedata.</p>
 */
public final class InstanceTemplate {

    private final int id;
    private final @Nullable LocalizedText name;

    public InstanceTemplate(int id, @Nullable LocalizedText name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    /**
     * Localized instance name; {@code null} when the host build supplied none.
     */
    public @Nullable LocalizedText getName() {
        return name;
    }

    public Builder toBuilder() {
        return new Builder().id(id).name(name);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof InstanceTemplate)) return false;
        InstanceTemplate that = (InstanceTemplate) o;
        return id == that.id && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }

    @Override
    public String toString() {
        return "InstanceTemplate[id=" + id + ", name=" + name + "]";
    }

    public static final class Builder {
        private int id;
        private @Nullable LocalizedText name;

        public Builder id(int id) {
            this.id = id;
            return this;
        }

        public Builder name(@Nullable LocalizedText name) {
            this.name = name;
            return this;
        }

        public InstanceTemplate build() {
            return new InstanceTemplate(id, name);
        }
    }
}
