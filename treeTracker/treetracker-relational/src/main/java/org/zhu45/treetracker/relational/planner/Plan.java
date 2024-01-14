package org.zhu45.treetracker.relational.planner;

import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;

public class Plan
{
    private final PlanNode root;
    private PlanStatistics planStatistics;
    private Map<Integer, List<Operator>> node2Operators;

    public Plan(PlanNode root)
    {
        this.root = requireNonNull(root, "root is null");
        this.planStatistics = new PlanStatistics();
    }

    public Plan(PlanNode root, PlanStatistics existingPlanStatistics)
    {
        this.root = requireNonNull(root, "root is null");
        this.planStatistics = requireNonNull(existingPlanStatistics, "existingPlanStatistics is null");
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public static List<SchemaTableName> getSchemaTableNames(PlanNode root)
    {
        List<SchemaTableName> res = new ArrayList<>();
        getSchemaTableNamesHelper(root, res);
        return res;
    }

    private static void getSchemaTableNamesHelper(PlanNode root, List<SchemaTableName> res)
    {
        if (root == null) {
            return;
        }
        switch (root.getNodeType()) {
            case join:
                JoinNode joinNode = (JoinNode) root;
                getSchemaTableNamesHelper(joinNode.getLeft(), res);
                getSchemaTableNamesHelper(joinNode.getRight(), res);
                break;
            case table:
                TableNode tableNode = (TableNode) root;
                res.add(tableNode.getSchemaTableName());
                break;
            case fullReducer:
                FullReducerNode fullReducerNode = (FullReducerNode) root;
                getSchemaTableNamesHelper(fullReducerNode.getSink(), res);
                break;
            default:
                throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, root.getNodeType() + "unimplemented");
        }
    }

    /**
     * This function calls swap() of join operator in the plan.
     */
    public void swapAll()
    {
        requireNonNull(root.getOperator(), "no operator bind to the plan; given plan is logical");
        // We assume that once the root has operator, the rest of nodes have operators as well. Thus, we don't perform
        // additional check.
        swapAllHelper(root);
    }

    private void swapAllHelper(PlanNode root)
    {
        if (root == null) {
            return;
        }
        if (root.getNodeType().equals(OptType.join)) {
            JoinNode joinNode = (JoinNode) root;
            joinNode.getOperator().swap();
            swapAllHelper(joinNode.getLeft());
            swapAllHelper(joinNode.getRight());
        }
    }

    public PlanStatistics getPlanStatistics()
    {
        return planStatistics;
    }

    public List<Operator> getOperatorList()
    {
        List<Operator> operatorList = new ArrayList<>();
        getOperatorListHelper(root, operatorList);
        return operatorList;
    }

    private static void getOperatorListHelper(PlanNode root, List<Operator> res)
    {
        if (root == null) {
            return;
        }
        switch (root.getNodeType()) {
            case join:
                JoinNode joinNode = (JoinNode) root;
                res.add(joinNode.getOperator());
                getOperatorListHelper(joinNode.getLeft(), res);
                getOperatorListHelper(joinNode.getRight(), res);
                break;
            case table:
                TableNode tableNode = (TableNode) root;
                res.add(tableNode.getOperator());
                break;
            case fullReducer:
                FullReducerNode fullReducerNode = (FullReducerNode) root;
                res.add(fullReducerNode.getOperator());
                getOperatorListHelper(fullReducerNode.getSink(), res);
                break;
            default:
                throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, root.getNodeType() + "unimplemented");
        }
    }

    public void setPlanStatistics(PlanStatistics planStatistics)
    {
        this.planStatistics = planStatistics;
    }

    public void setNode2Operators(Map<Integer, List<Operator>> node2Operators)
    {
        this.node2Operators = node2Operators;
    }

    public Map<Integer, List<Operator>> getNode2Operators()
    {
        return node2Operators;
    }
}
