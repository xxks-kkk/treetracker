package org.zhu45.treetracker.jdbc;

import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.ColumnMetadata;
import org.zhu45.treetracker.common.type.Type;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public final class JdbcColumnHandle
        implements ColumnHandle
{
    private final String connectorId;
    private final String columnName;
    private final JdbcTypeHandle jdbcTypeHandle;
    private final Type columnType;
    private final boolean nullable;

    public JdbcColumnHandle(
            String connectorId,
            String columnName,
            JdbcTypeHandle jdbcTypeHandle,
            Type columnType,
            boolean nullable)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.jdbcTypeHandle = requireNonNull(jdbcTypeHandle, "jdbcTypeHandle is null");
        this.columnType = requireNonNull(columnType, "columnType is null");
        this.nullable = nullable;
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public String getColumnName()
    {
        return columnName;
    }

    public JdbcTypeHandle getJdbcTypeHandle()
    {
        return jdbcTypeHandle;
    }

    @Override
    public Type getColumnType()
    {
        return columnType;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public ColumnMetadata getColumnMetadata()
    {
        return new ColumnMetadata(columnName, columnType, nullable, null, null, false, emptyMap());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JdbcColumnHandle o = (JdbcColumnHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.columnName, o.columnName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, columnName);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("connectorId", connectorId)
                .add("columnName", columnName)
                .add("jdbcTypeHandle", jdbcTypeHandle)
                .add("columnType", columnType)
                .add("nullable", nullable)
                .toString();
    }
}
