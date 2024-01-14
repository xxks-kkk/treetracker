package org.zhu45.treetracker.benchmark.micro;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.Arrays;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.Database.getTableNode;
import static org.zhu45.treetracker.benchmark.micro.MicroBenchDatabase.microbenchSchemaName;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class MicroBenchTwoWayJoinQueryGraph
{
    private MicroBenchTwoWayJoinQueryGraph() {}

    public static MultiwayJoinOrderedGraph constructJoinTreeForQuery(String relationA, String relationB)
    {
        MultiwayJoinNode rA = getTableNode(new SchemaTableName(microbenchSchemaName, relationA));
        MultiwayJoinNode rB = getTableNode(new SchemaTableName(microbenchSchemaName, relationB));

        return getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(rB, rA)), rB);
    }
}
