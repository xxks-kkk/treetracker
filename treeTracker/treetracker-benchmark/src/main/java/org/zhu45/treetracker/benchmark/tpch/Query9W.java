package org.zhu45.treetracker.benchmark.tpch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getNation;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getOrders;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPart;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getPartsupp;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getSupplier;

public class Query9W
        extends Query
{
    public Query9W(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode ordersNode = getOrders(TPCHQueries.Q9W);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q9W);
        MultiwayJoinNode supplierNode = getSupplier(TPCHQueries.Q9W);
        MultiwayJoinNode nationNode = getNation(TPCHQueries.Q9W, null);
        MultiwayJoinNode partNode = getPart(TPCHQueries.Q9W);
        MultiwayJoinNode partSuppNode = getPartsupp(TPCHQueries.Q9W);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(lineItemNode, ordersNode),
                asQueryGraphEdge(lineItemNode, partNode),
                asQueryGraphEdge(partNode, partSuppNode),
                asQueryGraphEdge(partSuppNode, supplierNode),
                asQueryGraphEdge(supplierNode, nationNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, lineItemNode);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
