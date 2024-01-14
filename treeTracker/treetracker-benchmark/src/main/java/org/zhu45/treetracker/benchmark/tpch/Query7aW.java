package org.zhu45.treetracker.benchmark.tpch;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.List;

import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getCustomer;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getLineitem;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getNation;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getOrders;
import static org.zhu45.treetracker.benchmark.tpch.TPCHDatabase.getSupplier;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Query7aW
        extends Query
{
    public Query7aW(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode nationNode = getNation(TPCHQueries.Q7aW, TableInstanceId.ONE);
        MultiwayJoinNode nationNode2 = getNation(TPCHQueries.Q7aW, TableInstanceId.TWO);
        MultiwayJoinNode customerNode = getCustomer(TPCHQueries.Q7aW);
        MultiwayJoinNode lineItemNode = getLineitem(TPCHQueries.Q7aW);
        MultiwayJoinNode supplierNode = getSupplier(TPCHQueries.Q7aW);
        MultiwayJoinNode ordersNode = getOrders(TPCHQueries.Q7aW);

        return createPhysicalPlanFromJoinOrdering(getJoinOrderingFromNodes(
                List.of(nationNode, nationNode2, customerNode, lineItemNode, supplierNode, ordersNode)));
    }
}
