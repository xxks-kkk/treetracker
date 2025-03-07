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
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getCustomerInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getDateInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getLineOrderInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getPartInt;
import static org.zhu45.treetracker.benchmark.ssb.SSBDatabase.getSupplierInt;

public class Query4P2FindOptJoinTree
        extends Query
{
    public Query4P2FindOptJoinTree(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode lineOrderNode = getLineOrderInt(SSBQueries.Q4P2);
        MultiwayJoinNode dateNode = getDateInt(SSBQueries.Q4P2);
        MultiwayJoinNode customerNode = getCustomerInt(SSBQueries.Q4P2);
        MultiwayJoinNode supplierNode = getSupplierInt(SSBQueries.Q4P2);
        MultiwayJoinNode partNode = getPartInt(SSBQueries.Q4P2);

        Pair<Plan, List<Operator>> pair = createPhysicalPlanFromPostgresPlan(Paths.get(SSB_UPDATED_POSTGRES_PLAN_STORED_PATH, "updated_4P2.json").toString(), List.of(lineOrderNode.getSchemaTableName(), customerNode.getSchemaTableName(), supplierNode.getSchemaTableName(), dateNode.getSchemaTableName(), partNode.getSchemaTableName()));

        return pair;
    }
}
