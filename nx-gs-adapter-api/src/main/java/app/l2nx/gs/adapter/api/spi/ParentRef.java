package app.l2nx.gs.adapter.api.spi;

import java.util.Objects;

/**
 * Declares that rows of the declaring entity belong to a row of another
 * declared entity: "every row of this entity references one
 * {@link #parentEntityName()} row via {@link #fkColumn()} on this entity's
 * primary table" (e.g. item &rarr; {@code of("character", "owner_id")}).
 *
 * <p>Consumed by the db-sync force-resync cascade: a row-level resync of the
 * parent entity with {@code cascade=true} resolves the dependent child rows
 * via {@code SELECT <pkColumn> FROM <primaryTable> WHERE <fkColumn> IN (...)}
 * and invalidates them alongside the requested parent rows, so the platform
 * receives a consistent re-publication of the parent and everything hanging
 * off it.</p>
 *
 * <p>Constraints (validated at module start, failure fails the module):
 * {@link #fkColumn()} must match {@code [A-Za-z_][A-Za-z0-9_]{0,63}} — it is
 * interpolated into SQL without quoting; {@link #parentEntityName()} must be
 * the {@link EntityMapping#entityName()} of another entity declared by the
 * same provider.</p>
 */
public final class ParentRef {

    private final String parentEntityName;
    private final String fkColumn;

    private ParentRef(String parentEntityName, String fkColumn) {
        this.parentEntityName = Objects.requireNonNull(parentEntityName,
                "ParentRef.parentEntityName is required");
        this.fkColumn = Objects.requireNonNull(fkColumn,
                "ParentRef.fkColumn is required");
    }

    public static ParentRef of(String parentEntityName, String fkColumn) {
        return new ParentRef(parentEntityName, fkColumn);
    }

    /**
     * {@link EntityMapping#entityName()} of the entity this entity's rows
     * belong to. Must reference an entity declared by the same provider.
     */
    public String parentEntityName() {
        return parentEntityName;
    }

    /**
     * Column on the declaring entity's primary table holding the parent's PK.
     */
    public String fkColumn() {
        return fkColumn;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParentRef)) return false;
        ParentRef that = (ParentRef) o;
        return parentEntityName.equals(that.parentEntityName)
                && fkColumn.equals(that.fkColumn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parentEntityName, fkColumn);
    }

    @Override
    public String toString() {
        return "ParentRef[parentEntityName=" + parentEntityName
                + ", fkColumn=" + fkColumn + "]";
    }
}
