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
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getNation;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getOrders;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getSupplier;

public class Query21
        extends Query
{
    public Query21(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q21);
        MultiwayJoinNode supplierNode = getSupplier(TPCHQueries.Q21);
        MultiwayJoinNode nationNode = getNation(TPCHQueries.Q21, null);
        MultiwayJoinNode ordersNode = getOrders(TPCHQueries.Q21);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(lineItemNode, supplierNode),
                asQueryGraphEdge(lineItemNode, ordersNode),
                asQueryGraphEdge(supplierNode, nationNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, lineItemNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(lineItemNode.getSchemaTableName(),
                        supplierNode.getSchemaTableName(),
                        nationNode.getSchemaTableName(),
                        ordersNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
