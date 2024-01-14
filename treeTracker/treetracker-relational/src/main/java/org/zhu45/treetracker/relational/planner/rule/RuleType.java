package org.zhu45.treetracker.relational.planner.rule;

public enum RuleType
{
    // Rule can be applied in place of plan generation, e.g., modify a specific node
    IN_PLACE,
    // Rule can only be applied iteratively after the whole plan generated, e.g., modify the overall plan structure
    AS_A_WHOLE
}
