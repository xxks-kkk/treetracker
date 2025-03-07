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
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getPartInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getSupplierInt;

public class Query2P1FindOptJoinTree
        extends Query
{
    public Query2P1FindOptJoinTree(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode lineOrderNode = getLineOrderInt(SSBQueries.Q2P1);
        MultiwayJoinNode dateNode = getDateInt(SSBQueries.Q2P1);
        MultiwayJoinNode partNode = getPartInt(SSBQueries.Q2P1);
        MultiwayJoinNode supplierNode = getSupplierInt(SSBQueries.Q2P1);

        Pair<Plan, List<Operator>> pair = createPhysicalPlanFromPostgresPlan(Paths.get(SSB_UPDATED_POSTGRES_PLAN_STORED_PATH, "updated_2P1.json").toString(), List.of(lineOrderNode.getSchemaTableName(), dateNode.getSchemaTableName(), partNode.getSchemaTableName(), supplierNode.getSchemaTableName()));

        return pair;
    }
}
