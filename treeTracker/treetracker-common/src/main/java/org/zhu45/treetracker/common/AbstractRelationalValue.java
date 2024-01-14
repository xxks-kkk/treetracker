package org.zhu45.treetracker.common;

import org.zhu45.treetracker.common.type.Type;

public abstract class AbstractRelationalValue
        implements RelationalValue
{
    protected Type type;

    public Type getType()
    {
        return type;
    }
}
