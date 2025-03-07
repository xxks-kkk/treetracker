package org.zhu45.treetracker.relational.planner;

import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.ACTUAL_ROWS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.AGGREGATE_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.ALIAS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.GATHER_MERGE_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.GATHER_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.HASH_JOIN_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.HASH_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.INNER_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.MATERIALIZE_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.MERGE_JOIN_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.NESTED_LOOP_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.NODE_TYPE_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.OUTER_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PARENT_RELATIONSHIP_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PLANS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PLAN_ROWS_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.PLAN_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.RELATION_NAME_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.SEQ_SCAN_VALUE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.Constants.SORT_VALUE;

/**
 * Field key appeared in Json format of Postgres plan.
 */
public enum PostgresPlanProperty
{
    NODE_TYPE(NODE_TYPE_VALUE),
    SEQ_SCAN(SEQ_SCAN_VALUE),
    HASH_JOIN(HASH_JOIN_VALUE),
    MERGE_JOIN(MERGE_JOIN_VALUE),
    NESTED_LOOP(NESTED_LOOP_VALUE),
    AGGREGATE(AGGREGATE_VALUE),
    GATHER(GATHER_VALUE),
    GATHER_MERGE(GATHER_MERGE_VALUE),
    SORT(SORT_VALUE),
    HASH(HASH_VALUE),
    PLANS(PLANS_VALUE),
    PLAN(PLAN_VALUE),
    PARENT_RELATIONSHIP(PARENT_RELATIONSHIP_VALUE),
    INNER(INNER_VALUE),
    OUTER(OUTER_VALUE),
    RELATION_NAME(RELATION_NAME_VALUE),
    PLAN_ROWS(PLAN_ROWS_VALUE),
    ACTUAL_ROWS(ACTUAL_ROWS_VALUE),
    ALIAS(ALIAS_VALUE),
    MATERIALIZE(MATERIALIZE_VALUE);

    public static class Constants
    {
        public static final String NODE_TYPE_VALUE = "Node Type";
        public static final String SEQ_SCAN_VALUE = "Seq Scan";
        public static final String HASH_JOIN_VALUE = "Hash Join";
        public static final String MERGE_JOIN_VALUE = "Merge Join";
        public static final String NESTED_LOOP_VALUE = "Nested Loop";
        public static final String AGGREGATE_VALUE = "Aggregate";
        public static final String GATHER_VALUE = "Gather";
        public static final String GATHER_MERGE_VALUE = "Gather Merge";
        public static final String HASH_VALUE = "Hash";
        public static final String SORT_VALUE = "Sort";
        public static final String PLANS_VALUE = "Plans";
        public static final String PLAN_VALUE = "Plan";
        public static final String PARENT_RELATIONSHIP_VALUE = "Parent Relationship";
        public static final String INNER_VALUE = "Inner";
        public static final String OUTER_VALUE = "Outer";
        public static final String RELATION_NAME_VALUE = "Relation Name";
        public static final String PLAN_ROWS_VALUE = "Plan Rows";
        public static final String ACTUAL_ROWS_VALUE = "Actual Rows";
        public static final String ALIAS_VALUE = "Alias";
        public static final String MATERIALIZE_VALUE = "Materialize";
    }

    private final String value;

    PostgresPlanProperty(String value)
    {
        this.value = value;
    }

    public String getValue()
    {
        return value;
    }

    public static PostgresPlanProperty getNodeType(String value)
    {
        switch (value) {
            case NODE_TYPE_VALUE:
                return NODE_TYPE;
            case SEQ_SCAN_VALUE:
                return SEQ_SCAN;
            case HASH_JOIN_VALUE:
                return HASH_JOIN;
            case NESTED_LOOP_VALUE:
                return NESTED_LOOP;
            case AGGREGATE_VALUE:
                return AGGREGATE;
            case GATHER_VALUE:
                return GATHER;
            case GATHER_MERGE_VALUE:
                return GATHER_MERGE;
            case HASH_VALUE:
                return HASH;
            case MERGE_JOIN_VALUE:
                return MERGE_JOIN;
            case SORT_VALUE:
                return SORT;
            case MATERIALIZE_VALUE:
                return MATERIALIZE;
            default:
                throw new IllegalArgumentException(value);
        }
    }

    @Override
    public String toString()
    {
        return value;
    }
}
