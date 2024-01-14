package org.zhu45.treetracker.benchmark.micro.exp2p9O;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.List;

public class Exp2P9Query50P
        extends Exp2P9OQueryTemplate
{
    public Exp2P9Query50P(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        return constructQuery(50);
    }
}
