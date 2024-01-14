package org.zhu45.treetracker.relational.planner.cost;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.util.List;

import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;

/**
 * Give the true cost of join tree measured by
 * the execution time.
 */
public class JoinTreeTrueCostProvider
        implements JoinTreeCostProvider
{
    // number of executions for each ordering to gather the execution time
    private static final int executionTimes = 2;

    @Override
    public JoinTreeCostReturn getCost(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context)
    {
        long executionTime = 0;
        for (int i = 0; i < executionTimes; ++i) {
            Plan plan = createPhysicalPlanFromJoinOrdering(ordering, joinTree, context);
            ExecutionNormal executionNormal = new ExecutionNormal(plan.getRoot());
            long currentTime = System.nanoTime();
            executionNormal.evalForBenchmark();
            executionTime += (System.nanoTime() - currentTime);
        }
        return JoinTreeCostReturn.builder(executionTime / (float) executionTimes).build();
    }

    @Override
    public boolean isUseTrueCard()
    {
        throw new UnsupportedOperationException();
    }

    public static Plan createPhysicalPlanFromJoinOrdering(JoinOrdering joinOrdering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context)
    {
        PlanBuildContext.Builder builder = builder();
        builder.setRules(List.of())
                .setPlanNodeIdAllocator(context.getPlanNodeIdAllocator())
                .setOperatorMap(context.getOperatorMap())
                .setJdbcClient(context.getJdbcClient())
                .setOrderedGraph(joinTree);
        PlanBuildContext planBuildContext = builder.build();
        PlanBuilder planBuilder = new PlanBuilder(joinOrdering.getSchemaTableNameList(), planBuildContext);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(planBuildContext);
        return physicalPlanBuilder.build(logicalPlan.getRoot());
    }
}
