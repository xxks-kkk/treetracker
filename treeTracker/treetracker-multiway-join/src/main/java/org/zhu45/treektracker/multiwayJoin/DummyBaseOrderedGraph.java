package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.DummyDomain;
import org.zhu45.treetracker.common.DummyNode;
import org.zhu45.treetracker.common.DummyValue;

import java.util.HashMap;
import java.util.List;

public class DummyBaseOrderedGraph
        extends BaseOrderedGraph<DummyNode, DummyValue, DummyDomain>
{
    public DummyBaseOrderedGraph(DummyNode root, HashMap<DummyNode, List<DummyNode>> children, HashMap<DummyNode, List<DummyNode>> parent, List<DummyNode> traversalList)
    {
        super(root, children, parent, traversalList);
    }

    public DummyBaseOrderedGraph(DummyBaseOrderedGraph og)
    {
        super(og);
    }
}
