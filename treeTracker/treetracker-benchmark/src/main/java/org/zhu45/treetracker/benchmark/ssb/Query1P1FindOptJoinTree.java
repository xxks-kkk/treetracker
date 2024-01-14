package org.zhu45.treetracker.benchmark.ssb;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getDateInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getLineOrderInt;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Query1P1FindOptJoinTree
        extends Query
{
    public Query1P1FindOptJoinTree(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode lineOrderNode = getLineOrderInt(SSBQueries.Q1P1);
        MultiwayJoinNode dateNode = getDateInt(SSBQueries.Q1P1);

        Pair<Plan, List<Operator>> pair = createPhysicalPlanFromJoinOrdering(getJoinOrderingFromNodes(List.of(dateNode, lineOrderNode)));
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(dateNode.getSchemaTableName(),
                lineOrderNode.getSchemaTableName()));
        return pair;
    }
}
