package org.zhu45.treetracker.relational.planner.plan;

import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.PlanVisitor;

import java.util.List;

/*
 * This is specific for Gather node in Postgres plan.
 */
public class GatherNode
        extends PlanNode
{
    private static final OptType NODE_PLAN_NODE_TYPE = OptType.gather;
    private PlanNode childPlanNode;

    public GatherNode(PlanNodeId id, PlanNode child, Side side)
    {
        super(id, side);

        this.childPlanNode = child;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return List.of(childPlanNode);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGather(this, context);
    }

    @Override
    public OptType getNodeType()
    {
        return NODE_PLAN_NODE_TYPE;
    }
}
