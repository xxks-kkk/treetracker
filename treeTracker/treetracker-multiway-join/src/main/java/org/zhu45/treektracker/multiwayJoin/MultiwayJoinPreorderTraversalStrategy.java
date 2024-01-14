package org.zhu45.treektracker.multiwayJoin;

import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.row.Row;

import java.util.List;

public class MultiwayJoinPreorderTraversalStrategy
        extends PreorderTraversalStrategy<MultiwayJoinNode, Row, MultiwayJoinDomain, MultiwayJoinOrderedGraph>
{
    public MultiwayJoinPreorderTraversalStrategy(MultiwayJoinNode root)
    {
        super(root, MultiwayJoinOrderedGraph.class);
    }

    public static MultiwayJoinOrderedGraph getMultiwayJoinOrderedGraph(List<? extends Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeLists, MultiwayJoinNode root)
    {
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return getMultiwayJoinOrderedGraph(g, root);
    }

    public static MultiwayJoinOrderedGraph getMultiwayJoinOrderedGraph(MultiwayJoinGraph g, MultiwayJoinNode root)
    {
        MultiwayJoinPreorderTraversalStrategy strategy = new MultiwayJoinPreorderTraversalStrategy(root);
        return strategy.traversal();
    }
}
