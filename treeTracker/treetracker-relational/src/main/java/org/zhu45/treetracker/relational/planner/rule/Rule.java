package org.zhu45.treetracker.relational.planner.rule;

import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;

public interface Rule
{
    Plan applyToLogicalPlan(Plan plan, PlanBuildContext context);
    Plan applyToPhysicalPlan(PlanNode node, RuleContext context);
    boolean checkForRulePrecondition(PlanNode node);
    RuleType getRuleType();
    RuleStatistics getRuleStatistics();
}
