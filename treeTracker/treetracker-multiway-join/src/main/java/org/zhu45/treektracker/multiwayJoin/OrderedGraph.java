package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.common.Node;
import org.zhu45.treetracker.common.Value;

import java.util.HashMap;
import java.util.List;

public interface OrderedGraph<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>>
{
    int getWidth();

    List<T> getTraversalList();

    T getRoot();

    HashMap<T, List<T>> getParent();

    HashMap<T, List<T>> getChildren();

    boolean isOgATree();
}
