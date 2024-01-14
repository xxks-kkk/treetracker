package org.zhu45.treetracker.benchmark.micro;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.List;

import static org.zhu45.treetracker.benchmark.micro.MicroBenchDatabase.ss1dr05mt40000sa1sb40000A;
import static org.zhu45.treetracker.benchmark.micro.MicroBenchDatabase.ss1dr05mt40000sa1sb40000B;
import static org.zhu45.treetracker.benchmark.micro.MicroBenchTwoWayJoinQueryGraph.constructJoinTreeForQuery;

public class TwoWayJoinQuery1
        extends Query
{
    public TwoWayJoinQuery1(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        return createFixedPhysicalPlanFromQueryGraph(constructJoinTreeForQuery(ss1dr05mt40000sa1sb40000A, ss1dr05mt40000sa1sb40000B));
    }
}
