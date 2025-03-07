package org.zhu45.treetracker.relational.planner;

import lombok.Builder;
import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.planner.plan.Side;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPlanBuilder
{
    @Test
    public void testPlanBuilderPlan1()
    {
        List<SchemaTableName> schemaTableNameList = new ArrayList<>(
                Arrays.asList(new SchemaTableName("test", "R"), new SchemaTableName("test", "T"),
                        new SchemaTableName("test", "S"), new SchemaTableName("test", "U")));
        PlanBuildContext context = PlanBuildContext.builder().setPlanNodeIdAllocator(
                new PlanNodeIdAllocator()).setRules(List.of()).build();
        PlanBuilder planBuilder = new PlanBuilder(schemaTableNameList, context);
        Plan plan = planBuilder.build();
        // The expected node types with post-order traversal on the left-deep join tree over the four relations
        LinkedList<OptType> expectedNodeTypes = new LinkedList<>(
                Arrays.asList(OptType.table, OptType.table, OptType.join, OptType.table, OptType.join, OptType.table,
                        OptType.join));
        assertTrue(new TestingPlanVisitor().visitPlan(plan.getRoot(), expectedNodeTypes));
    }

    @Test
    public void testPostgresPlan()
            throws IOException
    {
        String postgresPlan = getTestPostgresPlanFromResource("10a.json");
        PlanBuildContext context = PlanBuildContext.builder()
                .setPlanNodeIdAllocator(new PlanNodeIdAllocator())
                .setRules(List.of())
                .planBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES)
                .postgresPlan(postgresPlan)
                .schema("public")
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan plan = planBuilder.build();
        PlanPrinter printer = new PlanPrinter(plan.getRoot());
        assertEquals(
                "- [18]Join 16 join 17\n" + "    - [16]Gather Gather 15\n" + "        - [15]Join 0 join 14\n" + "    "
                        + "        - [0]Table public.char_name\n" + "            - [14]Hash Hash 13\n" + "           "
                        + "  " + "   - [13]Join 1 join 12\n" + "                    - [1]Table public.title\n" + "   " +
                        "  " + "       " + "        - [12]Hash Hash 11\n" + "                        - [11]Join 5 " +
                        "join 10\n" + "        " + "                    - [5]Join 2 join 4\n" + "                    " +
                        "            - [2]Table " + "public.cast_info\n" + "                                - [4]Hash" +
                        " Hash 3\n" + "              " + "                      - [3]Table public.role_type\n" + "   " +
                        "                         - " + "[10]Hash Hash 9\n" + "                                - " +
                        "[9]Join 6 join 8\n" + "             " + "                       - [6]Table public" +
                        ".movie_companies\n" + "                             " + "       - [8]Hash Hash 7\n" + "     " +
                        "                                   - [7]Table public" + ".company_name\n" + "    - [17]Table" +
                        " public.company_type\n",
                printer.toText(0));
    }

    @Test
    public void testCorrectSide()
            throws IOException
    {
        String postgresPlan = getTestPostgresPlanFromResource("10a.json");
        PlanBuildContext context = PlanBuildContext.builder()
                .setPlanNodeIdAllocator(new PlanNodeIdAllocator())
                .setRules(List.of())
                .planBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES)
                .postgresPlan(postgresPlan)
                .schema("public")
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan plan = planBuilder.build();
        {
            NodeSpecification titleNode = NodeSpecification.builder()
                    .optType(OptType.table)
                    .relationName("title")
                    .build();
            NodeSpecification hashNode = NodeSpecification.builder()
                    .optType(OptType.hash)
                    .build();
            NodeSpecification rootSpecification = NodeSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(titleNode, hashNode))
                    .build();
            PlanNode target = findTargetPlanNode(plan.getRoot(), rootSpecification);
            assertEquals(Side.INNER, target.getSide());
        }
        {
            NodeSpecification hashJoinNode = NodeSpecification.builder()
                    .optType(OptType.join)
                    .build();
            NodeSpecification hashNode = NodeSpecification.builder()
                    .optType(OptType.hash)
                    .build();
            NodeSpecification rootSpecification = NodeSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(hashJoinNode, hashNode))
                    .build();
            PlanNode target = findTargetPlanNode(plan.getRoot(), rootSpecification);
            assertEquals(Side.INNER, target.getSide());
        }
        {
            NodeSpecification tableNode = NodeSpecification.builder()
                    .optType(OptType.table)
                    .relationName("movie_companies")
                    .build();
            NodeSpecification hashNode = NodeSpecification.builder()
                    .optType(OptType.hash)
                    .build();
            NodeSpecification rootSpecification = NodeSpecification.builder()
                    .optType(OptType.join)
                    .children(List.of(tableNode, hashNode))
                    .build();
            PlanNode target = findTargetPlanNode(plan.getRoot(), rootSpecification);
            assertEquals(Side.INNER, target.getSide());
        }
    }

    @Builder
    public static class NodeSpecification
    {
        OptType optType;
        String relationName;
        List<NodeSpecification> children;
    }

    public static PlanNode findTargetPlanNode(PlanNode root, NodeSpecification rootSpecification)
    {
        if (isMatch(root, rootSpecification)) {
            return root;
        }
        for (PlanNode child : root.getSources()) {
            PlanNode find = findTargetPlanNode(child, rootSpecification);
            if (find != null) {
                return find;
            }
        }
        return null;
    }

    public static boolean isMatch(PlanNode candidateNode, NodeSpecification targetNode)
    {
        boolean typeMatch = candidateNode.getNodeType() == targetNode.optType;
        boolean nameMatch;
        boolean childrenMatch = true;
        if (candidateNode.getSchemaTableName() != null && targetNode.relationName != null) {
            nameMatch = candidateNode.getSchemaTableName().getTableName().equals(targetNode.relationName);
        }
        else {
            nameMatch = candidateNode.getSchemaTableName() == null && targetNode.relationName == null;
        }
        if (targetNode.children != null) {
            if (candidateNode.getSources().size() != targetNode.children.size()) {
                childrenMatch = false;
            }
            else {
                for (int i = 0; i < candidateNode.getSources().size(); i++) {
                    childrenMatch = childrenMatch && isMatch(candidateNode.getSources().get(0), targetNode.children.get(0));
                }
            }
        }
        return childrenMatch && typeMatch && nameMatch;
    }

    public static String getTestPostgresPlanFromResource(String jsonName)
            throws IOException
    {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream is = classLoader.getResourceAsStream(jsonName);
        byte[] data = is.readAllBytes();
        return new String(data);
    }
}
