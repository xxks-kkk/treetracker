package org.zhu45.treetracker.benchmark.tpch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getNation;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPart;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPartsupp;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getRegion;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getSupplier;

public class Query2
        extends Query
{
    public Query2(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode partNode = getPart(TPCHQueries.Q2);
        MultiwayJoinNode supplierNode = getSupplier(TPCHQueries.Q2);
        MultiwayJoinNode partSuppNode = getPartsupp(TPCHQueries.Q2);
        MultiwayJoinNode nationNode = getNation(TPCHQueries.Q2, null);
        MultiwayJoinNode regionNode = getRegion(TPCHQueries.Q2);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(partNode, partSuppNode),
                asQueryGraphEdge(partSuppNode, supplierNode),
                asQueryGraphEdge(supplierNode, nationNode),
                asQueryGraphEdge(nationNode, regionNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, partNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(partNode.getSchemaTableName(),
                        partSuppNode.getSchemaTableName(),
                        supplierNode.getSchemaTableName(),
                        nationNode.getSchemaTableName(),
                        regionNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
