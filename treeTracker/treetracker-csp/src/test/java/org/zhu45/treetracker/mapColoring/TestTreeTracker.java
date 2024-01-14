package org.zhu45.treetracker.mapColoring;

import org.junit.jupiter.api.Test;
import org.zhu45.treektracker.multiwayJoin.PreorderTraversalStrategy;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Constraints;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.Graph;
import org.zhu45.treetracker.common.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class TestTreeTracker {
    @Test
    void TestTreeTracker2Map1() {
        MapColoringDomain domain = new MapColoringDomain(
                new ColorValue(ColorValue.Color.Blue),
                new ColorValue(ColorValue.Color.Red),
                new ColorValue(ColorValue.Color.Green));
        MapColoringNode a = new MapColoringNode("A", new MapColoringDomain(domain));
        MapColoringNode b = new MapColoringNode("B", new MapColoringDomain(domain));
        MapColoringNode c = new MapColoringNode("C", new MapColoringDomain(domain));
        MapColoringNode d = new MapColoringNode("D", new MapColoringDomain(domain));
        MapColoringNode e = new MapColoringNode("E", new MapColoringDomain(domain));
        MapColoringNode f = new MapColoringNode("F", new MapColoringDomain(domain));

        List<Edge<MapColoringNode, ColorValue, MapColoringDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(a, b),
                asEdge(b, c),
                asEdge(b, d),
                asEdge(d, e),
                asEdge(d, f)));

        MapColoringGraph g = new MapColoringGraph(edgeLists);
        PreorderTraversalStrategy<MapColoringNode, ColorValue, MapColoringDomain, MapColoringOrderedGraph> strategy = new PreorderTraversalStrategy<>(
                b, MapColoringOrderedGraph.class);
        MapColoringConstraints constraints = new MapColoringConstraints();
        TreeTracker<MapColoringNode, ColorValue, MapColoringDomain, MapColoringOrderedGraph> tt2 = new TreeTracker<>(
                strategy, constraints);
        tt2.treeTracker2();
        assertTrue(checkAssignments(g, constraints));
    }

    /**
     * The graph used by this test case is from TT-2 paper Fig. 4
     */
    @Test
    void TestTreeTracker2Map2() {
        MapColoringDomain domain = new MapColoringDomain(
                new ColorValue(ColorValue.Color.Blue),
                new ColorValue(ColorValue.Color.Red),
                new ColorValue(ColorValue.Color.Green));
        MapColoringNode a = new MapColoringNode("a", new MapColoringDomain(domain));
        MapColoringNode b = new MapColoringNode("b", new MapColoringDomain(domain));
        MapColoringNode c = new MapColoringNode("c", new MapColoringDomain(domain));
        MapColoringNode d = new MapColoringNode("d", new MapColoringDomain(domain));
        MapColoringNode e = new MapColoringNode("e", new MapColoringDomain(domain));
        MapColoringNode f = new MapColoringNode("f", new MapColoringDomain(domain));
        MapColoringNode g = new MapColoringNode("g", new MapColoringDomain(domain));
        MapColoringNode h = new MapColoringNode("h", new MapColoringDomain(domain));
        MapColoringNode i = new MapColoringNode("i", new MapColoringDomain(domain));

        List<MapColoringNode> nodes = new ArrayList<>(Arrays.asList(a, b, c, d, e, f, g, h, i));

        List<Edge<MapColoringNode, ColorValue, MapColoringDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(a, b),
                asEdge(a, c),
                asEdge(a, d),
                asEdge(b, e),
                asEdge(c, f),
                asEdge(f, g),
                asEdge(f, h),
                asEdge(h, i)));

        MapColoringGraph graph = new MapColoringGraph(edgeLists);
        PreorderTraversalStrategy<MapColoringNode, ColorValue, MapColoringDomain, MapColoringOrderedGraph> strategy =
                new PreorderTraversalStrategy<>(a, MapColoringOrderedGraph.class);
        MapColoringConstraints constraints = new MapColoringConstraints();
        TreeTracker<MapColoringNode, ColorValue, MapColoringDomain, MapColoringOrderedGraph> tt2 = new TreeTracker<>(strategy, constraints);
        tt2.treeTracker2();
        assertTrue(checkAssignments(graph, constraints));
    }

    @Test
    void TestTreeTracker2Map3() {
        MapColoringDomain domain = new MapColoringDomain(
                new ColorValue(ColorValue.Color.Blue),
                new ColorValue(ColorValue.Color.Red),
                new ColorValue(ColorValue.Color.Green));
        MapColoringNode wa = new MapColoringNode("WA", domain);
        MapColoringNode nt = new MapColoringNode("NT", domain);
        MapColoringNode q = new MapColoringNode("Q", domain);
        MapColoringNode nsw = new MapColoringNode("NSW", domain);
        MapColoringNode v = new MapColoringNode("V", domain);
        MapColoringNode sa = new MapColoringNode("SA", domain);

        List<MapColoringNode> nodes = new ArrayList<>(Arrays.asList(wa, nt, q, nsw, v, sa));

        List<Edge<MapColoringNode, ColorValue, MapColoringDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(wa, sa),
                asEdge(nt, sa),
                asEdge(q, sa),
                asEdge(nsw, sa),
                asEdge(v, sa),
                asEdge(wa, nt),
                asEdge(nt, q),
                asEdge(q, nsw),
                asEdge(v, nsw)));

        MapColoringGraph graph = new MapColoringGraph(edgeLists);
        PreorderTraversalStrategy<MapColoringNode, ColorValue, MapColoringDomain, MapColoringOrderedGraph> strategy =
                new PreorderTraversalStrategy<>(wa, MapColoringOrderedGraph.class);
        MapColoringConstraints constraints = new MapColoringConstraints();
        TreeTracker<MapColoringNode, ColorValue, MapColoringDomain, MapColoringOrderedGraph> tt2 = new TreeTracker<>(strategy, constraints);
        Exception exception = assertThrows(IllegalArgumentException.class, tt2::treeTracker2);
        assertTrue(exception.getMessage().contains("constraint graph has width > 1"));
    }

    /**
     * Check the assignment with the given graph to see whether the assignment
     * satisfies constraint represented by graph
     * and constraint checking function.
     * 
     * @param g           CSP graph
     * @param constraints Constraint instance of problem domain
     * @return true if the assignments satisfy the constraint; false otherwise
     */
    public static boolean checkAssignments(Graph g, Constraints constraints) {
        for (Object onode : g.getNodes()) {
            BaseNode baseNode = (BaseNode) onode;
            if (baseNode.getAssignedValue() != null) {
                if (!constraints.isInstantiate(baseNode, (Value) baseNode.getAssignedValue())) {
                    return false;
                }
            } else {
                throw new IllegalArgumentException("Input graph contains unassigned baseNode: " + baseNode.toString());
            }
        }
        return true;
    }
}
