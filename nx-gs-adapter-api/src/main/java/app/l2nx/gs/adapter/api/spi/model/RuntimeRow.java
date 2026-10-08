package app.l2nx.gs.adapter.api.spi.model;

import java.util.Objects;

public final class RuntimeRow<T> {

    private final long pk;
    private final T dto;
    private final long stateStamp;

    public RuntimeRow(long pk, T dto) {
        this(pk, dto, 0L);
    }

    public RuntimeRow(long pk, T dto, long stateStamp) {
        this.pk = pk;
        this.dto = dto;
        this.stateStamp = stateStamp;
    }

    public long getPk() {
        return pk;
    }

    public T getDto() {
        return dto;
    }

    /**
     * Host-side change marker, folded into the change hash and never sent: forces an update for a change the hashed
     * DTO fields cannot show (e.g. an effect re-applied with identical fields). {@code 0} = no marker.
     */
    public long getStateStamp() {
        return stateStamp;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RuntimeRow)) return false;
        RuntimeRow<?> that = (RuntimeRow<?>) o;
        return pk == that.pk && stateStamp == that.stateStamp && Objects.equals(dto, that.dto);
    }

    @Override
    public int hashCode() {
        return Objects.hash(pk, dto, stateStamp);
    }

    @Override
    public String toString() {
        return "RuntimeRow[pk=" + pk + ", dto=" + dto + ", stateStamp=" + stateStamp + "]";
    }
}
