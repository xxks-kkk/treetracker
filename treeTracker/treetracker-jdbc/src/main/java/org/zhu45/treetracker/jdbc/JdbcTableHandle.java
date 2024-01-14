package org.zhu45.treetracker.jdbc;

import com.google.common.base.Joiner;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TableHandle;

import javax.annotation.Nullable;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class JdbcTableHandle
        implements TableHandle
{
    private final String connectorId;
    private final SchemaTableName schemaTableName;

    // catalog, schema and table names are reported by the remote database
    private final String catalogName;
    private final String schemaName;
    private final String tableName;

    public JdbcTableHandle(
            String connectorId,
            SchemaTableName schemaTableName,
            @Nullable String catalogName,
            @Nullable String schemaName,
            String tableName)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.catalogName = catalogName;
        this.schemaName = schemaName;
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public String getConnectorId()
    {
        return connectorId;
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName;
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

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        JdbcTableHandle o = (JdbcTableHandle) obj;
        return Objects.equals(this.connectorId, o.connectorId) &&
                Objects.equals(this.schemaTableName, o.schemaTableName);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(connectorId, schemaTableName);
    }

    @Override
    public String toString()
    {
        return Joiner.on(":").useForNull("null").join(connectorId, schemaTableName, catalogName, schemaName, tableName);
    }
}
