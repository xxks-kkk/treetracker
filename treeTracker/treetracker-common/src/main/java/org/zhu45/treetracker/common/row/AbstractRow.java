package org.zhu45.treetracker.common.row;

import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.type.Type;

import java.util.List;

public abstract class AbstractRow
        implements Row
{
    protected List<RelationalValue> vals;
    protected List<String> attributes;
    protected List<Type> types;
    protected boolean isGood;

    @Override
    public List<String> getAttributes()
    {
        return attributes;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void setIsGood(boolean b)
    {
        this.isGood = b;
    }

    @Override
    public boolean getIsGood()
    {
        return isGood;
    }
}
