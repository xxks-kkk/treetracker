package org.zhu45.treetracker.relational.planner;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.List;

import static org.zhu45.treetracker.common.Edge.asEdge;

public class TestJoinTreeEdgeWeight
{
    @Test
    public void test1() {
        MultiwayJoinNode node1 = new MultiwayJoinNode(new SchemaTableName("test", "R"),
                List.of("A", "B", "C"), new MultiwayJoinDomain());
        MultiwayJoinNode node2 = new MultiwayJoinNode(new SchemaTableName("test", "S"),
                List.of("B", "D"), new MultiwayJoinDomain());
        Edge<MultiwayJoinNode, Row, MultiwayJoinDomain> edge = asEdge(node1, node2, new JoinTreeEdgeWeight(node1, node2));
        Assertions.assertEquals(edge.getEdgeWeight().getWeight(), 1);
        List<String> edgeLabel = ((JoinTreeEdgeWeight) edge.getEdgeWeight()).getEdgeLabel();
        Assertions.assertEquals(List.of("B"), edgeLabel);
        // Check original attributes of nodes aren't modified
        Assertions.assertEquals(node1.getAttributes(), List.of("A", "B", "C"));
        Assertions.assertEquals(node2.getAttributes(), List.of("B", "D"));
    }
}
