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
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getCustomer;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getOrders;

public class Query3W
        extends Query
{
    public Query3W(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode customerNode = getCustomer(TPCHQueries.Q3W);
        MultiwayJoinNode ordersNode = getOrders(TPCHQueries.Q3W);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q3W);

        List<QueryGraphEdge> edgeLists = Arrays.asList(
                asQueryGraphEdge(lineItemNode, ordersNode),
                asQueryGraphEdge(ordersNode, customerNode));
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, lineItemNode);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
