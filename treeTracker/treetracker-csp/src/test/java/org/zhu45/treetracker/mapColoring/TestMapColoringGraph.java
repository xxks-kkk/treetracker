package org.zhu45.treetracker.mapColoring;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Edge;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.Edge.asEdge;

class TestMapColoringGraph
{
    /**
     * This test case is based on Australia Map Graph (Figure 6.1 of
     * Artificial Intelligence: A Modern Approach (3rd)). We modify the graph to ignore T.
     */
    @Test
    void australiaMapColoringProblemGraph() {
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
                asEdge(wa,sa),
                asEdge(nt, sa),
                asEdge(q, sa),
                asEdge(nsw, sa),
                asEdge(v, sa),
                asEdge(wa, nt),
                asEdge(nt, q),
                asEdge(q, nsw),
                asEdge(v, nsw)));

        MapColoringGraph g = new MapColoringGraph(edgeLists);
        Set<MapColoringNode> nodesSet = g.getNodes();
        assertEquals(nodesSet.size(), 6);
        for (MapColoringNode baseNode : nodesSet) {
            String connectedString = baseNode.connectedToString();
            switch (baseNode.getNodeName()) {
                case "WA":
                    assertEquals(baseNode.getConnected().size(), 2);
                    assertTrue(connectedString.contains("NT"));
                    assertTrue(connectedString.contains("SA"));
                    break;
                case "NT":
                    assertEquals(baseNode.getConnected().size(), 3);
                    assertTrue(connectedString.contains("WA"));
                    assertTrue(connectedString.contains("SA"));
                    assertTrue(connectedString.contains("Q"));
                    break;
                case "Q":
                    assertEquals(baseNode.getConnected().size(), 3);
                    assertTrue(connectedString.contains("NT"));
                    assertTrue(connectedString.contains("SA"));
                    assertTrue(connectedString.contains("NSW"));
                    break;
                case "NSW":
                    assertEquals(baseNode.getConnected().size(), 3);
                    assertTrue(connectedString.contains("Q"));
                    assertTrue(connectedString.contains("SA"));
                    assertTrue(connectedString.contains("V"));
                    break;
                case "V":
                    assertEquals(baseNode.getConnected().size(), 2);
                    assertTrue(connectedString.contains("SA"));
                    assertTrue(connectedString.contains("NSW"));
                    break;
                case "SA":
                    assertEquals(baseNode.getConnected().size(), 5);
                    assertTrue(connectedString.contains("WA"));
                    assertTrue(connectedString.contains("NT"));
                    assertTrue(connectedString.contains("Q"));
                    assertTrue(connectedString.contains("NSW"));
                    assertTrue(connectedString.contains("V"));
                    break;
            }
        }
    }
}
