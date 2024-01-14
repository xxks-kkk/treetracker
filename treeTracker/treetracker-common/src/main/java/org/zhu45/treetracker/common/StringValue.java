package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.type.Type;

import java.io.Serializable;

import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;

public class StringValue
        extends AbstractRelationalValue
        implements Value, Serializable
{
    private final String val;

    public StringValue(String val)
    {
        this.val = val;
        this.type = VARCHAR;
    }

    public StringValue(Type type, String val)
    {
        this.val = val;
        this.type = type;
    }

    public static StringValue of(String val)
    {
        return new StringValue(val);
    }

    @Override
    public String toString()
    {
        return this.val;
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
        StringValue o = (StringValue) obj;
        return this.val.equals(o.val);
    }

    @Override
    public int hashCode()
    {
        return val.hashCode();
    }
}
