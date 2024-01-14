package org.zhu45.treetracker.relational;

import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.type.Type;

import static java.util.Objects.requireNonNull;

public class JoinResultColumnHandle
        implements ColumnHandle
{
    private String name;
    private Type type;

    public JoinResultColumnHandle(String name, Type type)
    {
        this.name = requireNonNull(name, "Column has to have a name");
        this.type = requireNonNull(type, "A type has to be specified for column");
    }

    @Override
    public String getColumnName()
    {
        return this.name;
    }

    @Override
    public Type getColumnType()
    {
        return this.type;
    }

    @Override
    public String toString()
    {
        StringBuilder output = new StringBuilder();
        output.append("[").append(type.getDisplayName()).append(",").append(name).append("]");
        return output.toString();
    }
}
