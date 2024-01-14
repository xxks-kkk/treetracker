package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.JoinTreeGenerator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProvider;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;

import java.util.HashMap;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;

/**
 * Find the optimal join tree by executing all possible join trees for the given join ordering
 * gathered from the plan, which is assumed to be left-deep. The rule will ignore existing join tree
 * associated with the plan and replace it with the optimal join tree. This rule is designed to work with TTJ
 * given a join ordering determined priori by some other method. Such join ordering is assumed to be cross-product free.
 */
public class FindOptimalJoinTree
        extends BaseRule
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(FindOptimalJoinTree.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    // the join ordering of the plan that this rule is invoked on
    private JoinOrdering joinOrdering;
    private final JoinTreeCostProvider costProvider;
    private RuleStatistics.Builder builder = RuleStatistics.builder(this.getClass());

    public FindOptimalJoinTree(JoinTreeCostProvider costProvider)
    {
        this.costProvider = costProvider;
    }

    @Override
    public Plan applyToLogicalPlan(Plan plan, PlanBuildContext context)
    {
        return plan;
    }

    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        ruleContext = context;
        List<SchemaTableName> schemaTableNameList = getSchemaTableNames(node);
        joinOrdering = new JoinOrdering(schemaTableNameList);
        JoinTreeGenerator joinTreeGenerator = new JoinTreeGenerator(context.getPlanBuildContext());
        List<MultiwayJoinOrderedGraph> allJoinTrees = joinTreeGenerator.generateJoinTrees(schemaTableNameList);
        MultiwayJoinOrderedGraph optimalJoinTree = findTheMinimumExecutionCostJoinTree(allJoinTrees);
        // It's easier to just return a new Plan instead of modifying the existing plan
        Plan newPlan = createPhysicalPlanFromJoinOrdering(joinOrdering, optimalJoinTree);
        // Because multiple rules can be applied before this, we copy the existing plan planStatistics to the new plan so that we don't lose
        // the statistics coming from application of previous rules
        newPlan.setPlanStatistics(context.getPlanStatistics());
        ruleStatistics = builder
                .optimalJoinOrdering(joinOrdering)
                .build();
        return newPlan;
    }

    private MultiwayJoinOrderedGraph findTheMinimumExecutionCostJoinTree(List<MultiwayJoinOrderedGraph> joinTrees)
    {
        MultiwayJoinOrderedGraph minimumExecutionCostJoinTree = null;
        HashMap<MultiwayJoinOrderedGraph, Double> searchedJoinTrees = new HashMap<>();
        double minCost = Double.MAX_VALUE;
        for (MultiwayJoinOrderedGraph joinTree : joinTrees) {
            double cost = costProvider.getCost(joinOrdering, joinTree, ruleContext.getPlanBuildContext()).getCost();
            searchedJoinTrees.put(joinTree, cost);
            if (cost < minCost) {
                minCost = cost;
                minimumExecutionCostJoinTree = joinTree;
            }
        }
        checkState(minimumExecutionCostJoinTree != null, "minimumExecutionCostJoinTree cannot be null");
        builder = builder.searchedJoinTrees(searchedJoinTrees)
                .optimalJoinTree(minimumExecutionCostJoinTree)
                .cost(minCost);
        return minimumExecutionCostJoinTree;
    }

    @Override
    public RuleType getRuleType()
    {
        return RuleType.AS_A_WHOLE;
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        return node.isRoot() &&
                node.getNodeType() == OptType.join && // Root node of the given plan has to be a join
                (((JoinNode) node).getLeft().getNodeType() == OptType.join ||
                        (((JoinNode) node).getLeft().getNodeType() == OptType.table && ((JoinNode) node).getRight().getNodeType() == OptType.table)) &&  // The given plan is a left-deep plan
                TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.isAssignableFrom(node.getOperator().getClass()); // enforce the rule is only applicable to TTJ
    }
}
