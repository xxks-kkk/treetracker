package org.zhu45.treetracker.benchmark.micro.exp2p5;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.List;

public class Exp2P5Query70P
        extends Exp2P5QueryTemplate
{
    public Exp2P5Query70P(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        return constructQuery(70);
    }
}
