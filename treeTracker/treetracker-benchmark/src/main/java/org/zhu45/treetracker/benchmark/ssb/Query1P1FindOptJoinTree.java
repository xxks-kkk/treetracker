package org.zhu45.treetracker.benchmark.ssb;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.nio.file.Paths;
import java.util.List;

import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_UPDATED_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getDateInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getLineOrderInt;

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

        Pair<Plan, List<Operator>> pair = createPhysicalPlanFromPostgresPlan(Paths.get(SSB_UPDATED_POSTGRES_PLAN_STORED_PATH, "updated_1P1.json").toString(), List.of(lineOrderNode.getSchemaTableName(), dateNode.getSchemaTableName()));

        return pair;
    }
}
