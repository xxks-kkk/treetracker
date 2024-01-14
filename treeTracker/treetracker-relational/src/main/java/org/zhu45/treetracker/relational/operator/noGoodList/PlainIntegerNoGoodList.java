package org.zhu45.treetracker.relational.operator.noGoodList;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.zhu45.treetracker.common.IntegerValue;

public class PlainIntegerNoGoodList
        implements NoGoodList<IntegerValue>
{
    private final IntOpenHashSet noGoodList;

    private PlainIntegerNoGoodList()
    {
        noGoodList = new IntOpenHashSet();
    }

    @Override
    public int size()
    {
        return noGoodList.size();
    }

    @Override
    public boolean contains(IntegerValue value)
    {
        return noGoodList.contains(value.getVal());
    }

    @Override
    public boolean add(IntegerValue value)
    {
        return noGoodList.add(value.getVal());
    }

    public static NoGoodList<IntegerValue> create()
    {
        return new PlainIntegerNoGoodList();
    }

    @Override
    public String toString()
    {
        return noGoodList.toString();
    }
}
