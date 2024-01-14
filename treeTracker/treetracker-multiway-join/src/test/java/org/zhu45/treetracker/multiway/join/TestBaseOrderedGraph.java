package org.zhu45.treetracker.multiway.join;

import org.junit.jupiter.api.Test;
import org.zhu45.treektracker.multiwayJoin.DummyBaseOrderedGraph;
import org.zhu45.treetracker.common.BaseDomain;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treektracker.multiwayJoin.BaseOrderedGraph;
import org.zhu45.treetracker.common.DummyDomain;
import org.zhu45.treetracker.common.DummyNode;
import org.zhu45.treetracker.common.DummyValue;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.Graph;
import org.zhu45.treektracker.multiwayJoin.PreorderTraversalStrategy;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class TestBaseOrderedGraph {
    @Test
    public void testDepth() {
        DummyNode mc = new DummyNode("MC", null);
        DummyNode ci = new DummyNode("CI", null);
        DummyNode n = new DummyNode("N", null);
        DummyNode mk = new DummyNode("MK", null);
        DummyNode k = new DummyNode("K", null);
        DummyNode cn = new DummyNode("CN", null);
        DummyNode mi = new DummyNode("MI", null);
        DummyNode mii = new DummyNode("MII", null);
        DummyNode it = new DummyNode("IT", null);
        DummyNode t = new DummyNode("T", null);

        List<Edge<DummyNode, DummyValue, DummyDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(mc, ci),
                asEdge(ci, n),
                asEdge(mc, mk),
                asEdge(mk, k),
                asEdge(mc, cn),
                asEdge(mc, mi),
                asEdge(mc, mii),
                asEdge(mii, it),
                asEdge(mc, t)
        ));

        Graph<DummyNode, DummyValue, DummyDomain> g = new Graph<>(edgeLists);
        PreorderTraversalStrategy<DummyNode, DummyValue, DummyDomain, DummyBaseOrderedGraph> strategy = new PreorderTraversalStrategy<>(mc, DummyBaseOrderedGraph.class);
        DummyBaseOrderedGraph og = strategy.traversal();
        LinkedHashMap<Integer, List<DummyNode>> depth = og.getDepth();
        assertEquals(3, depth.size());
        assertEquals(Arrays.asList(mc), depth.get(0));
        assertEquals(Arrays.asList(ci, mk, cn, mi, mii, t), depth.get(1));
        assertEquals(Arrays.asList(n, k, it), depth.get(2));
    }

    @Test
    public void testGetPathLength()
    {
        DummyNode mc = new DummyNode("MC", null);
        DummyNode ci = new DummyNode("CI", null);
        DummyNode n = new DummyNode("N", null);
        DummyNode mk = new DummyNode("MK", null);
        DummyNode k = new DummyNode("K", null);
        DummyNode cn = new DummyNode("CN", null);
        DummyNode mi = new DummyNode("MI", null);
        DummyNode mii = new DummyNode("MII", null);
        DummyNode it = new DummyNode("IT", null);
        DummyNode t = new DummyNode("T", null);

        List<Edge<DummyNode, DummyValue, DummyDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(mc, ci),
                asEdge(ci, n),
                asEdge(mc, mk),
                asEdge(mk, k),
                asEdge(mc, cn),
                asEdge(mc, mi),
                asEdge(mc, mii),
                asEdge(mii, it),
                asEdge(mc, t)
        ));

        Graph<DummyNode, DummyValue, DummyDomain> g = new Graph<>(edgeLists);
        PreorderTraversalStrategy<DummyNode, DummyValue, DummyDomain, DummyBaseOrderedGraph> strategy = new PreorderTraversalStrategy<>(mc, DummyBaseOrderedGraph.class);
        DummyBaseOrderedGraph og = strategy.traversal();
        assertEquals(og.getPathLength(mc, ci), 1);
        assertEquals(og.getPathLength(mc, n), 2);
        assertEquals(og.getPathLength(ci, n), 1);
        assertEquals(og.getPathLength(mc, mk), 3);
        assertThrows(IllegalArgumentException.class, () -> og.getPathLength(cn, it));
        assertEquals(og.getPathLength(mii, it), 1);
        assertEquals(og.getPathLength(mc, t), 9);
    }

    @Test
    public void testCopyConstructor()
    {
        DummyNode b = new DummyNode("B", null);
        DummyNode s = new DummyNode("S", null);
        DummyNode r = new DummyNode("R", null);
        DummyNode t = new DummyNode("T", null);

        List<Edge<DummyNode, DummyValue, DummyDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(b, s),
                asEdge(b, r),
                asEdge(r, t)
        ));

        Graph<DummyNode, DummyValue, DummyDomain> g = new Graph<>(edgeLists);
        PreorderTraversalStrategy<DummyNode, DummyValue, DummyDomain, DummyBaseOrderedGraph> strategy = new PreorderTraversalStrategy<>(b, DummyBaseOrderedGraph.class);
        DummyBaseOrderedGraph og = strategy.traversal();

        DummyBaseOrderedGraph newOg = new DummyBaseOrderedGraph(og);

        List<DummyNode> allBaseNodes = List.of(b,s, r, t);
        for (DummyNode baseNode : allBaseNodes) {
            for (DummyNode newBaseNode : newOg.getTraversalList()) {
                if (baseNode.hashCode() == newBaseNode.hashCode()) {
                    assertNotSame(baseNode, newBaseNode, String.format("%s and %s are the same object", baseNode, newBaseNode));
                }
            }
        }

        // Ensures the copy constructor copies each node from og once.
        List<DummyNode> newAllBaseNodes = newOg.getTraversalList();
        newOg.getParent().keySet().forEach(key -> {
            for (DummyNode newBaseNode : newAllBaseNodes) {
                if (newBaseNode.hashCode() == key.hashCode()) {
                    assertSame(newBaseNode, key, newBaseNode.getNodeName() + " is copied more than once");
                }
            }
        });
        newOg.getChildren().keySet().forEach(key -> {
            for (DummyNode newBaseNode : newAllBaseNodes) {
                if (newBaseNode.hashCode() == key.hashCode()) {
                    assertSame(newBaseNode, key, newBaseNode.getNodeName() + " is copied more than once");
                }
            }
        });
    }
}
