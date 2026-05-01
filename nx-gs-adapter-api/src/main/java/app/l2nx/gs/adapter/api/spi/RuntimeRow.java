package app.l2nx.gs.adapter.api.spi;

import java.util.Objects;

/**
 * One row in a runtime-sync snapshot: the primary key (entity identity) plus
 * the typed DTO populated from live field accessors at snapshot time. Produced
 * by {@link RuntimeEntityMapping#snapshot()}.
 *
 * @param <T> wire DTO type for the entity
 */
public final class RuntimeRow<T> {

    private final long pk;
    private final T dto;

    public RuntimeRow(long pk, T dto) {
        this.pk = pk;
        this.dto = dto;
    }

    /**
     * Primary key — entity identity, matches the DTO's {@code id}-equivalent
     * field.
     */
    public long getPk() {
        return pk;
    }

    /**
     * Typed payload — passed to {@code SyncEvent.payload} on publish.
     */
    public T getDto() {
        return dto;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuntimeRow)) return false;
        RuntimeRow<?> that = (RuntimeRow<?>) o;
        return pk == that.pk && Objects.equals(dto, that.dto);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pk, dto);
    }

    @Override
    public String toString() {
        return "RuntimeRow[pk=" + pk + ", dto=" + dto + "]";
    }
}
