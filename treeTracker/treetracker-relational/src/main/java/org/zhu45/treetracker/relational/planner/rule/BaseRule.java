package org.zhu45.treetracker.relational.planner.rule;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;

import java.util.List;
import java.util.Set;

import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;

public abstract class BaseRule
        implements Rule
{
    protected RuleStatistics ruleStatistics;
    protected RuleContext ruleContext;

    @Override
    public Plan applyToLogicalPlan(Plan plan, PlanBuildContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RuleType getRuleType()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public RuleStatistics getRuleStatistics()
    {
        return ruleStatistics;
    }

    Plan createPhysicalPlanFromJoinOrdering(JoinOrdering joinOrdering, MultiwayJoinOrderedGraph joinTree)
    {
        PlanBuildContext.Builder builder = builder();
        builder.setRules(List.of())
                .setPlanNodeIdAllocator(ruleContext.getPlanNodeIdAllocator())
                .setOperatorMap(ruleContext.getOperatorMap())
                .setJdbcClient(ruleContext.getJdbcClient());
        if (joinTree != null) {
            builder.setOrderedGraph(joinTree);
        }
        PlanBuildContext context = builder.build();
        PlanBuilder planBuilder = new PlanBuilder(joinOrdering.getSchemaTableNameList(), context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        return physicalPlanBuilder.build(logicalPlan.getRoot());
    }

    protected static RuleType checkIfRuleTypeIsAllowed(Set<RuleType> allowedRuleType, RuleType specifiedRuleType)
    {
        if (!allowedRuleType.contains(specifiedRuleType)) {
            throw new RuntimeException("SpecifiedRuleType: " + specifiedRuleType + "is not allowed");
        }
        return specifiedRuleType;
    }
}
