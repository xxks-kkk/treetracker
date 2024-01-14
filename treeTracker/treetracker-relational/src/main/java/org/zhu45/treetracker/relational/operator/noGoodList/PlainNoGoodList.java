package org.zhu45.treetracker.relational.operator.noGoodList;

import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.zhu45.treetracker.common.Value;

import java.util.Set;

public class PlainNoGoodList
        implements NoGoodList<Value>
{
    private final Set<Value> noGoodList;

    private PlainNoGoodList()
    {
        noGoodList = UnifiedSet.newSet();
    }

    @Override
    public int size()
    {
        return noGoodList.size();
    }

    @Override
    public boolean contains(Value value)
    {
        return noGoodList.contains(value);
    }

    @Override
    public boolean add(Value value)
    {
        return noGoodList.add(value);
    }

    public static NoGoodList<Value> create()
    {
        return new PlainNoGoodList();
    }

    @Override
    public String toString()
    {
        return noGoodList.toString();
    }
}
