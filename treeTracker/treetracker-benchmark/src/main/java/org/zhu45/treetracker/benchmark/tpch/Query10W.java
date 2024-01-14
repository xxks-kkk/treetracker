package org.zhu45.treetracker.benchmark.tpch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getCustomer;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getNation;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getOrders;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query10W
        extends Query
{
    public Query10W(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode customerNode = getCustomer(TPCHQueries.Q10W);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q10W);
        MultiwayJoinNode ordersNode = getOrders(TPCHQueries.Q10W);
        MultiwayJoinNode nationNode = getNation(TPCHQueries.Q10W, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(lineItemNode, ordersNode),
                asEdge(ordersNode, customerNode),
                asEdge(customerNode, nationNode)), lineItemNode);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
