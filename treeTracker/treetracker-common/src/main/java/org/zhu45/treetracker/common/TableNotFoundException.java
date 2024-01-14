package org.zhu45.treetracker.common;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TableNotFoundException
        extends NotFoundException
{
    private final SchemaTableName tableName;

    public TableNotFoundException(SchemaTableName tableName)
    {
        this(tableName, format("Table '%s' not found", tableName));
    }

    public TableNotFoundException(SchemaTableName tableName, String message)
    {
        super(message);
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public TableNotFoundException(SchemaTableName tableName, Throwable cause)
    {
        this(tableName, format("Table '%s' not found", tableName), cause);
    }

    public TableNotFoundException(SchemaTableName tableName, String message, Throwable cause)
    {
        super(message, cause);
        this.tableName = requireNonNull(tableName, "tableName is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }
}
