package app.l2nx.gs.adapter.api.spi.model;

import java.util.Objects;

/**
 * Declares that every row of the declaring entity references one {@link #parentEntityName()} row via
 * {@link #fkColumn()} on its primary table (e.g. item &rarr; {@code of("character", "owner_id")}).
 * The force-resync cascade follows it.
 *
 * <p>Validated at module start (failure fails the module): {@link #fkColumn()} must match
 * {@code [A-Za-z_][A-Za-z0-9_]{0,63}} since it is interpolated into SQL unquoted;
 * {@link #parentEntityName()} must name another entity of the same provider.</p>
 */
public final class ParentRef {

    private final String parentEntityName;
    private final String fkColumn;

    private ParentRef(String parentEntityName, String fkColumn) {
        this.parentEntityName = Objects.requireNonNull(parentEntityName, "ParentRef.parentEntityName is required");
        this.fkColumn = Objects.requireNonNull(fkColumn, "ParentRef.fkColumn is required");
    }

    public static ParentRef of(String parentEntityName, String fkColumn) {
        return new ParentRef(parentEntityName, fkColumn);
    }

    public String parentEntityName() {
        return parentEntityName;
    }

    public String fkColumn() {
        return fkColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParentRef)) return false;
        ParentRef that = (ParentRef) o;
        return parentEntityName.equals(that.parentEntityName) && fkColumn.equals(that.fkColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentEntityName, fkColumn);
    }

    @Override
    public String toString() {
        return "ParentRef[parentEntityName=" + parentEntityName + ", fkColumn=" + fkColumn + "]";
    }
}
