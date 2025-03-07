package org.zhu45.treetracker.relational.planner.testing.postgresplan;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.commons.lang3.tuple.Pair;
import org.eclipse.collections.impl.factory.Strings;
import org.zhu45.treetracker.common.TreeTrackerException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Stack;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.commons.lang3.ArrayUtils.subarray;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_USER_ERROR;
import static org.zhu45.treetracker.relational.planner.testing.postgresplan.PostgresPlanGenerator.MockPlanToJson.generateMockPlanToJson;

/**
 * This class mocks a postgres plan based on a given simple specification. This helps to test bushy plan support based on Postgres plan.
 * The high-level steps are:
 * 1. parse the specification
 * 2. generate internal Postgres plan based on specification
 * 3. serialize the generated Postgres plan into JSON string.
 * <p>
 * We illustrate the specification through an example. Suppose we have the following bushy plan:
 * <pre>
 *                              \join
 *                             /     \
 *                           \join    \join
 *                          /   \     /  \
 *                         R    S    U    V
 * </pre>
 * The above bushy plan can be expressed using our specification as:
 * <pre>
 * JOIN,Outer,1,1
 * |JOIN,Outer,10,100
 * ||TAB,R,Outer,10,10
 * ||TAB,S,Inner,6,10
 * |JOIN,Inner,11,5
 * ||TAB,U,Inner,10,10
 * ||TAB,V,Outer,5,5
 * </pre>
 * The specification is very much similar to how we represent a join tree. The number of "|" represent parent-child relationship
 * in the plan, which is identical to how we represent a join tree. Besides "|", each line contains
 * - a OpNode, which can be JOIN,TAB,HASH,GATHER,AGG. This corresponds to "Node Type" in Postgres plan.
 * - relation: This only shows up in TAB. This corresponds to "Relation Name" in Postgres plan.
 * - Outer/Inner: specify side. This corresponds to "Parent Relationship" in Postgres plan.
 * - Plan Rows: the number of rows estimated by Postgres for the node. This corresponds to "Plan Rows" in Postgres plan.
 * - Actual Rows: the number of rows computed by Postgres for the node. This corresponds to "Actual Rows" in Postgres plan.
 * Note that both Plan Rows and Actual Rows fields are optional.
 * <p>
 * For example, consider the line
 * <pre>
 *     |JOIN,Inner,11,5
 * </pre>
 * The line says the node is a join. Since it is a join node, there is no relation associated with the node. Its side is Inner.
 * The plan rows is 11 and the actual rows is 5.
 * <p>
 * Consider another example, consider the line
 * <pre>
 *     ||TAB,R,Outer,10,10
 * </pre>
 * The line says the node is a table. Since it is a table scan node, there is a relation. Its side is Outer. Both plan rows and actual rows are 10.
 * The following is how to represent 10a.json using our specification:
 * <pre>
 * AGG,Outer
 * |JOIN,Outer
 * ||GATHER,Outer
 * |||JOIN,Outer
 * ||||TAB,char_name,Outer
 * ||||HASH,Inner
 * |||||JOIN,Outer
 * ||||||TAB,title,Outer
 * ||||||HASH,Inner
 * |||||||JOIN,Outer
 * ||||||||JOIN,Outer
 * |||||||||TAB,cast_info,Outer
 * |||||||||HASH,Inner
 * ||||||||||TAB,role_type,Outer
 * ||||||||HASH,Inner
 * |||||||||JOIN,Outer
 * ||||||||||TAB,movie_companies,Outer
 * ||||||||||HASH,Inner
 * |||||||||||TAB,company_name,Outer
 * ||TAB,company_type,Inner
 * </pre>
 */
public class PostgresPlanGenerator
{
    private PostgresPlanGenerator() {}

    private enum NodeType
    {
        AGG,
        JOIN,
        GATHER,
        GATHER_MERGE,
        TAB,
        HASH,
        SORT,
        MATERIALIZE
    }

    public static String generatePostgresPlanJson(String postgresPlanSpec)
    {
        Stack<Pair<MockNode, Integer>> stack = new Stack<>();
        String[] rows = postgresPlanSpec.split("\n");
        for (String row : rows) {
            String[] fields = row.split(",");
            int depth = extractDepth(fields[0]);
            MockNode node = createNode(subarray(fields, 0, fields.length));
            if (!stack.isEmpty()) {
                Pair<MockNode, Integer> pair = stack.pop();
                while (pair.getValue() >= depth) {
                    pair = stack.pop();
                }
                pair.getKey().getPlans().add(node);
                stack.push(pair);
                stack.push(Pair.of(node, depth));
            }
            else {
                stack.push(Pair.of(node, depth));
            }
        }
        MockNode rootNode = null;
        while (!stack.isEmpty()) {
            rootNode = stack.pop().getKey();
        }
        return generateMockPlanToJson(new MockPlanNode(rootNode));
    }

    private static int extractDepth(String field)
    {
        if (field.contains("|")) {
            return Strings.asChars(field).count(c -> c == '|');
        }
        else if (EnumSet.allOf(NodeType.class).stream().map(NodeType::name).collect(Collectors.toList()).contains(field)) {
            return 0;
        }
        else {
            throw new TreeTrackerException(GENERIC_USER_ERROR, "Unknown specification field: " + field);
        }
    }

    private static MockNode createNode(String[] fields)
    {
        String cleanedUp = fields[0].replaceAll("\\|", "");
        switch (NodeType.valueOf(cleanedUp)) {
            case AGG:
                MockAggregateNode.MockAggregateNodeBuilder builder = MockAggregateNode.builder();
                builder.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder.side(fields[1]);
                    builder.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder.side(fields[1]);
                    builder.planRows(Long.parseLong(fields[2]));
                    builder.actualRows(Long.parseLong(fields[3]));
                }
                return builder.build();
            case GATHER:
                MockGatherNode.MockGatherNodeBuilder builder2 = MockGatherNode.builder();
                builder2.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder2.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder2.side(fields[1]);
                    builder2.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder2.side(fields[1]);
                    builder2.planRows(Long.parseLong(fields[2]));
                    builder2.actualRows(Long.parseLong(fields[3]));
                }
                return builder2.build();
            case HASH:
                MockHashNode.MockHashNodeBuilder builder3 = MockHashNode.builder();
                builder3.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder3.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder3.side(fields[1]);
                    builder3.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder3.side(fields[1]);
                    builder3.planRows(Long.parseLong(fields[2]));
                    builder3.actualRows(Long.parseLong(fields[3]));
                }
                return builder3.build();
            case JOIN:
                MockJoinNode.MockJoinNodeBuilder builder4 = MockJoinNode.builder();
                builder4.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder4.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder4.side(fields[1]);
                    builder4.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder4.side(fields[1]);
                    builder4.planRows(Long.parseLong(fields[2]));
                    builder4.actualRows(Long.parseLong(fields[3]));
                }
                return builder4.build();
            case TAB:
                MockTableNode.MockTableNodeBuilder builder5 = MockTableNode.builder();
                checkState(fields.length >= 3, "there aren't enough fields: " + fields);
                if (fields.length == 3) {
                    builder5.relationName(fields[1]);
                    builder5.side(fields[2]);
                }
                else if (fields.length == 4) {
                    builder5.relationName(fields[1]);
                    builder5.side(fields[2]);
                    builder5.planRows(Long.parseLong(fields[3]));
                }
                else if (fields.length == 5) {
                    builder5.relationName(fields[1]);
                    builder5.side(fields[2]);
                    builder5.planRows(Long.parseLong(fields[3]));
                    builder5.actualRows(Long.parseLong(fields[4]));
                }
                return builder5.build();
            case SORT:
                MockSortNode.MockSortNodeBuilder builder6 = MockSortNode.builder();
                builder6.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder6.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder6.side(fields[1]);
                    builder6.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder6.side(fields[1]);
                    builder6.planRows(Long.parseLong(fields[2]));
                    builder6.actualRows(Long.parseLong(fields[3]));
                }
                return builder6.build();
            case MATERIALIZE:
                MockMaterializeNode.MockMaterializeNodeBuilder builder7 = MockMaterializeNode.builder();
                builder7.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder7.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder7.side(fields[1]);
                    builder7.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder7.side(fields[1]);
                    builder7.planRows(Long.parseLong(fields[2]));
                    builder7.actualRows(Long.parseLong(fields[3]));
                }
                return builder7.build();
            case GATHER_MERGE:
                MockGatherMergeNode.MockGatherMergeNodeBuilder builder8 = MockGatherMergeNode.builder();
                builder8.plans(new ArrayList<>());
                if (fields.length == 2) {
                    builder8.side(fields[1]);
                }
                else if (fields.length == 3) {
                    builder8.side(fields[1]);
                    builder8.planRows(Long.parseLong(fields[2]));
                }
                else if (fields.length == 4) {
                    builder8.side(fields[1]);
                    builder8.planRows(Long.parseLong(fields[2]));
                    builder8.actualRows(Long.parseLong(fields[3]));
                }
                return builder8.build();
            default:
                throw new TreeTrackerException(GENERIC_USER_ERROR, "Unknown specification field: " + fields[0]);
        }
    }

    public static class MockPlanToJson
    {
        private ObjectMapper objectMapper;

        private MockPlanToJson()
        {
            this.objectMapper = new ObjectMapper();
            objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        }

        public static String generateMockPlanToJson(MockNode planRoot)
        {
            MockPlanToJson mockPlanToJson = new MockPlanToJson();
            try {
                ArrayNode rootNode = mockPlanToJson.objectMapper.createArrayNode();
                rootNode.add(mockPlanToJson.objectMapper.valueToTree(planRoot));
                return mockPlanToJson.objectMapper.writeValueAsString(rootNode);
            }
            catch (IOException e) {
                throw new TreeTrackerException(GENERIC_USER_ERROR, "Exception happened when write plan to JSON\n" + e);
            }
        }
    }
}
