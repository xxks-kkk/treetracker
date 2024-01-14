package org.zhu45.treetracker.relational.operator.noGoodList;

import org.zhu45.treetracker.common.Value;

public interface NoGoodList<T extends Value>
{
    int size();

    boolean contains(T value);

    boolean add(T value);

    static NoGoodList create()
    {
        throw new UnsupportedOperationException();
    }
}
