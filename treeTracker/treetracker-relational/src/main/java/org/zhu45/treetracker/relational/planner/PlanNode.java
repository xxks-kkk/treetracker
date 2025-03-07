package org.zhu45.treetracker.relational.planner;

import lombok.Getter;
import lombok.Setter;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.plan.Side;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public abstract class PlanNode
{
    private final PlanNodeId id;
    private Operator operator;
    private boolean isRoot;
    @Getter @Setter
    private Side side;
    @Getter @Setter
    private long planRows;
    @Getter @Setter
    private long actualRows;
    @Getter @Setter
    protected SchemaTableName schemaTableName;
    @Getter @Setter
    protected SchemaTableName virtualSchemaTableName;

    protected PlanNode(PlanNodeId id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
    }

    protected PlanNode(PlanNodeId id, Side side)
    {
        requireNonNull(id, "id is null");
        requireNonNull(side, "side is null");
        this.id = id;
        this.side = side;
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
        List<PlanNode> sources = getSources();
        List<Operator> operators = new ArrayList<>();
        for (PlanNode source : sources) {
            switch (source.getNodeType()) {
                case join:
                case table:
                    operators.add(source.getOperator());
                    break;
                case hash:
                case gather:
                case sort:
                case materialize:
                case gather_merge:
                case aggregate:
                    checkState(source.getSources().size() == 1, source.getNodeType() + " node must have one child");
                    PlanNode targetNode = source.getSources().get(0);
                    while (targetNode.getOperator() == null) {
                        checkState(targetNode.getSources().size() == 1, targetNode
                                + "has more than one child but it has no binding operator");
                        targetNode = targetNode.getSources().get(0);
                    }
                    operators.add(targetNode.getOperator());
                    break;
                case fullReducer:
                    throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, "full reducer node should not be a child of any node");
            }
        }
        this.operator.setChildren(operators);
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
