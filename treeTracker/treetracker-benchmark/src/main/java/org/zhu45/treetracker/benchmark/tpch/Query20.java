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
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPart;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPartsupp;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getSupplier;

public class Query20
        extends Query
{
    public Query20(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode partSuppNode = getPartsupp(TPCHQueries.Q20);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q20);
        MultiwayJoinNode supplierNode = getSupplier(TPCHQueries.Q20);
        MultiwayJoinNode nationNode = getNation(TPCHQueries.Q20, null);
        MultiwayJoinNode partNode = getPart(TPCHQueries.Q20);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(lineItemNode, supplierNode),
                asQueryGraphEdge(supplierNode, partSuppNode),
                asQueryGraphEdge(partSuppNode, partNode),
                asQueryGraphEdge(supplierNode, nationNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, lineItemNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(lineItemNode.getSchemaTableName(),
                        supplierNode.getSchemaTableName(),
                        partSuppNode.getSchemaTableName(),
                        partNode.getSchemaTableName(),
                        nationNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
