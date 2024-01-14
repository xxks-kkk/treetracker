package org.zhu45.treetracker.multiway.join;

import org.junit.jupiter.api.Test;
import org.zhu45.treektracker.multiwayJoin.DummyBaseOrderedGraph;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treektracker.multiwayJoin.BaseOrderedGraph;
import org.zhu45.treetracker.common.DummyDomain;
import org.zhu45.treetracker.common.DummyNode;
import org.zhu45.treetracker.common.DummyValue;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.Graph;
import org.zhu45.treektracker.multiwayJoin.PreorderTraversalStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class TestPreorderTraversalStrategy {
    @Test
    void preorderTraversal() {
        DummyNode a = new DummyNode("A", null);
        DummyNode b = new DummyNode("B", null);
        DummyNode c = new DummyNode("C", null);
        DummyNode d = new DummyNode("D", null);
        DummyNode e = new DummyNode("E", null);
        DummyNode f = new DummyNode("F", null);

        List<Edge<DummyNode, DummyValue, DummyDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(a, b),
                asEdge(b, c),
                asEdge(b, d),
                asEdge(d, e),
                asEdge(d, f)
        ));

        Graph<DummyNode, DummyValue, DummyDomain> g = new Graph<>(edgeLists);
        PreorderTraversalStrategy<DummyNode, DummyValue, DummyDomain, DummyBaseOrderedGraph> strategy = new PreorderTraversalStrategy<>(b, DummyBaseOrderedGraph.class);
        DummyBaseOrderedGraph og = strategy.traversal();
        assertEquals(og.getWidth(), 1);
        assertEquals(og.getTraversalList().toString(), "[B, A, C, D, E, F]");
    }
}
