package org.zhu45.treetracker.common;

import java.io.Serializable;

import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;

public class IntegerValue
        extends AbstractRelationalValue
        implements Value, Serializable
{
    private final int val;

    public IntegerValue(int val)
    {
        this.val = val;
        this.type = INTEGER;
    }

    @Override
    public String toString()
    {
        return this.val + "";
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
        IntegerValue o = (IntegerValue) obj;
        return this.val == o.val;
    }

    @Override
    public int hashCode()
    {
        return val;
    }

    public int getVal()
    {
        return val;
    }

    public static IntegerValue of(int val)
    {
        return new IntegerValue(val);
    }
}
