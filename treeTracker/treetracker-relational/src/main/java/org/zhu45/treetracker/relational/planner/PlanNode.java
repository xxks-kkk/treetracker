package org.zhu45.treetracker.relational.planner;

import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.Operator;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public abstract class PlanNode
{
    private final PlanNodeId id;
    private Operator operator;
    private boolean isRoot;

    protected PlanNode(PlanNodeId id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
        this.operator = null;
    }

    public PlanNodeId getId()
    {
        return id;
    }

    /**
     * Get the upstream PlanNodes (i.e., children) of the current PlanNode.
     */
    public abstract List<PlanNode> getSources();

    /**
     * A visitor pattern interface to operate on IR.
     */
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }

    public Operator getOperator()
    {
        return operator;
    }

    public void setOperator(Operator operator)
    {
        checkArgument(operator.getOperatorType().equals(getNodeType()),
                String.format("Operator type: %s and the logical plan node type: %s has to match",
                        operator.getOperatorType(), getNodeType()));
        this.operator = operator;
        this.operator.setChildren(getSources().stream().map(PlanNode::getOperator).collect(Collectors.toList()));
    }

    public void setRoot()
    {
        isRoot = true;
    }

    public boolean isRoot()
    {
        return isRoot;
    }

    public abstract OptType getNodeType();
}
