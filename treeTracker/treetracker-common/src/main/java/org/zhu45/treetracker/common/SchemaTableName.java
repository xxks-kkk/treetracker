package org.zhu45.treetracker.common;

import java.io.Serializable;
import java.util.Objects;

public class SchemaTableName
        implements Serializable
{
    private final String schemaName;
    private final String tableName;

    public SchemaTableName(String schemaName, String tableName)
    {
        this.schemaName = SchemaUtil.checkNotEmpty(schemaName, "schemaName");
        this.tableName = SchemaUtil.checkNotEmpty(tableName, "tableName");
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(schemaName, tableName);
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
        final SchemaTableName other = (SchemaTableName) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString()
    {
        return schemaName + '.' + tableName;
    }

    public SchemaTablePrefix toSchemaTablePrefix()
    {
        return new SchemaTablePrefix(schemaName, tableName);
    }
}
