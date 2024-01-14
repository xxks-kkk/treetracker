package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.common.Node;
import org.zhu45.treetracker.common.Value;

public interface TraversalStrategy<T extends Node<T, V, D>, V extends Value, D extends Domain<D, V>, G extends OrderedGraph<T, V, D>>
{
    G traversal();
}
