package app.l2nx.gs.adapter.api.kafka.sync.gd.instancetemplate;

import app.l2nx.gs.adapter.api.localization.LocalizedText;
import java.util.Objects;
import org.jspecify.annotations.Nullable;

/**
 * {@code id -> name} catalog of instanced zones, resolving the {@code instanceId} of {@code CharacterInstanceCooldownDbDto}.
 * {@code name} is converted to the platform {@code LocalizedText} consumer-side in nx-gamedata.
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
