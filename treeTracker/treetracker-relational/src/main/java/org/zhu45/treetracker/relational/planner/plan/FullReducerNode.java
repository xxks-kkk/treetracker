package org.zhu45.treetracker.relational.planner.plan;

import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeId;
import org.zhu45.treetracker.relational.planner.PlanVisitor;

import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * This node represents full reducer step.
 */
public class FullReducerNode
        extends PlanNode
{
    private static final OptType NODE_PLAN_NODE_TYPE = OptType.fullReducer;
    private final List<PlanNode> planNodes;
    private PlanNode sink;

    public FullReducerNode(PlanNodeId id, List<PlanNode> planNodes)
    {
        super(id);

        requireNonNull(planNodes, "planNodes is null");

        this.planNodes = planNodes;
    }

    public void setSink(PlanNode sink)
    {
        this.sink = sink;
    }

    public PlanNode getSink()
    {
        return this.sink;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return Collections.singletonList(sink);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitFullReducer(this, context);
    }

    @Override
    public OptType getNodeType()
    {
        return NODE_PLAN_NODE_TYPE;
    }
}
