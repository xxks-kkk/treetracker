package org.zhu45.treetracker.relational.execution;

import org.apache.commons.collections4.MultiSet;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.PlanNode;

import java.util.List;

public abstract class ExecutionBase
{
    private PlanNode root;

    /**
     * Execute a physical plan
     *
     * @param root the root node of a physical plan
     */
    public ExecutionBase(PlanNode root)
    {
        this.root = root;
    }

    public PlanNode getRoot()
    {
        return this.root;
    }

    public void setRoot(PlanNode root)
    {
        this.root = root;
    }

    public abstract void open();

    /**
     * Actually evaluate the physical plan and obtain the
     * one row of the result set.
     */
    public abstract Row getNext();

    public abstract void close();

    public abstract MultiSet<Row> eval();

    public static void cleanUp(List<Operator> operators)
    {
        operators.forEach(operator -> {
            if (operator.getMultiwayJoinNode() != null) {
                MultiwayJoinNode node = operator.getMultiwayJoinNode();
                node.getDomain().close();
            }
            operator.close();
        });
    }
}
