package org.zhu45.treetracker.jdbc;

import com.google.common.collect.ImmutableList;
import org.zhu45.treetracker.common.type.Type;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class JdbcOutputTableHandle
{
    /**
     * Example that helps to understand the purpose of each field:
     * <p>
     * Suppose we create a table like following via JDBC:
     * <p>
     * presto> create table postgresql.public.orders3 (orderkey bigint);
     * <p>
     * Then, the each field will be instantiated to:
     * - connectorID   = postgresql
     * - catalogName   = postgres
     * - schemaName    = public
     * - tableName     = order3
     * - columnNames   = <skip>self-explanatory</skip>
     * - columnTypes   = <skip>self-explanatory</skip>
     * - relationName  = order3
     */
    private final String connectorId;
    private final String catalogName;
    private final String schemaName;
    private final String tableName;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final String relationName;

    public JdbcOutputTableHandle(
            String connectorId,
            @Nullable String catalogName,
            @Nullable String schemaName,
            String tableName,
            List<String> columnNames,
            List<Type> columnTypes,
            String relationName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.relationName = requireNonNull(relationName, "relationName is null");

        requireNonNull(columnNames, "columnNames is null");
        requireNonNull(columnTypes, "columnTypes is null");
        checkArgument(columnNames.size() == columnTypes.size(), "columnNames and columnTypes sizes don't match");
        this.columnNames = ImmutableList.copyOf(columnNames);
        this.columnTypes = ImmutableList.copyOf(columnTypes);
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    @Nullable
    public String getCatalogName()
    {
        return catalogName;
    }

    @Nullable
    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    public List<String> getColumnNames()
    {
        return columnNames;
    }

    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    public String getRelationName()
    {
        return relationName;
    }

    @Override
    public String toString()
    {
        return format("jdbc:%s.%s.%s", catalogName, schemaName, tableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(
                connectorId,
                catalogName,
                schemaName,
                tableName,
                columnNames,
                columnTypes,
                relationName);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JdbcOutputTableHandle other = (JdbcOutputTableHandle) obj;
        return Objects.equals(this.connectorId, other.connectorId) &&
                Objects.equals(this.catalogName, other.catalogName) &&
                Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName) &&
                Objects.equals(this.columnNames, other.columnNames) &&
                Objects.equals(this.columnTypes, other.columnTypes) &&
                Objects.equals(this.relationName, other.relationName);
    }
}
