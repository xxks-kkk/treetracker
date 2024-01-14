package org.zhu45.treetracker.relational.planner;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.OptType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPlanBuilder
{
    @Test
    public void testPlanBuilderPlan1()
    {
        List<SchemaTableName> schemaTableNameList = new ArrayList<>(
                Arrays.asList(
                        new SchemaTableName("test", "R"),
                        new SchemaTableName("test", "T"),
                        new SchemaTableName("test", "S"),
                        new SchemaTableName("test", "U")
                )
        );
        PlanBuildContext context = PlanBuildContext.builder()
                .setPlanNodeIdAllocator(new PlanNodeIdAllocator())
                .setRules(List.of())
                .build();
        PlanBuilder planBuilder = new PlanBuilder(schemaTableNameList, context);
        Plan plan = planBuilder.build();
        // The expected node types with post-order traversal on the left-deep join tree over the four relations
        LinkedList<OptType> expectedNodeTypes = new LinkedList<>(Arrays.asList(
                OptType.table, OptType.table, OptType.join,
                OptType.table, OptType.join, OptType.table, OptType.join
        ));
        assertTrue(new TestingPlanVisitor().visitPlan(plan.getRoot(), expectedNodeTypes));
    }
}
