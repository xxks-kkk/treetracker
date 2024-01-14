package org.zhu45.treetracker.common;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

public interface Domain<T extends Domain<T, V>, V extends Value>
        extends Iterable<V>, Serializable
{
    Iterator<V> iterator();

    boolean remove(V v);

    int size();

    boolean add(V val);

    void clear();

    List<V> getDomainAsList();

    boolean isEmpty();

    Domain<T, V> deepCopy();
}
