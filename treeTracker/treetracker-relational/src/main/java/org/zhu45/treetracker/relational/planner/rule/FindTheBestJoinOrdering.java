package org.zhu45.treetracker.relational.planner.rule;

import de.renebergelt.test.Switches;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;
import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.TableScanVisitor.gatherTableScanNodes;

/**
 * Find the best join ordering for HJ by executing all possible join ordering
 * for the relations in the plan and pick the ordering with the minimal execution time.
 * There are two constraints: the given plan is assumed to be left-deep and no ordering
 * with cross product is considered. Note this rule serves no practice use because it already
 * executes the given query; it is only used for empirical benchmark on TTJ vs. HJ.
 */
public class FindTheBestJoinOrdering
        extends BaseRule
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(FindTheBestJoinOrdering.class);

    private HashMap<SchemaTableName, HashSet<String>> schemaTableNameAttributes;
    private HashMap<JoinOrdering, Float> searchedPlan;
    // number of executions for each ordering to gather the execution time
    private static final int executionTimes = 2;
    private RuleStatistics.Builder builder = RuleStatistics.builder(this.getClass());

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
        schemaTableNameAttributes = populateSchemaTableNameAttributes(schemaTableNameList, context.getJdbcClient());
        List<JoinOrdering> allPossibleOrderingsWithoutCrossProduct = generateAllPossibleOrderingsWithoutCrossProduct(schemaTableNameList);
        JoinOrdering optimalJoinOrdering = findTheMinimumExecutionCostOrdering(allPossibleOrderingsWithoutCrossProduct);
        modifyPlanBasedOnOptimalJoinOrdering(node, optimalJoinOrdering);
        ruleStatistics = builder.build();
        return new Plan(node, context.getPlanStatistics());
    }

    /**
     * Each joinOrdering returned is assumed to be a list of schemaTableName that starts from left-most relation and all the way to the top.
     */
    private List<JoinOrdering> generateAllPossibleOrderingsWithoutCrossProduct(List<SchemaTableName> schemaTableNameList)
    {
        List<JoinOrdering> res = new ArrayList<>();
        int[] visited = new int[schemaTableNameList.size()];
        Arrays.fill(visited, 0);
        generateAllPossibleOrderingsWithoutCrossProductHelper(schemaTableNameList, new ArrayList<>(), visited, res);
        return res;
    }

    private void generateAllPossibleOrderingsWithoutCrossProductHelper(List<SchemaTableName> schemaTableNameList,
            List<SchemaTableName> tmp,
            int[] visited,
            List<JoinOrdering> res)
    {
        if (tmp.size() == schemaTableNameList.size()) {
            res.add(new JoinOrdering(new ArrayList<>(tmp)));
            return;
        }
        for (int i = 0; i < schemaTableNameList.size(); ++i) {
            if (visited[i] == 0 && checkIfSchemaTableNameCanBeAddedToTheOrdering(tmp, schemaTableNameList.get(i))) {
                visited[i] = 1;
                tmp.add(schemaTableNameList.get(i));
                generateAllPossibleOrderingsWithoutCrossProductHelper(schemaTableNameList, tmp, visited, res);
                tmp.remove(tmp.size() - 1);
                visited[i] = 0;
            }
        }
    }

    public static HashMap<SchemaTableName, HashSet<String>> populateSchemaTableNameAttributes(List<SchemaTableName> schemaTableNameList, JdbcClient jdbcClient)
    {
        HashMap<SchemaTableName, HashSet<String>> schemaTableNameAttributes = new HashMap<>();
        for (SchemaTableName schemaTableName : schemaTableNameList) {
            schemaTableNameAttributes.put(schemaTableName, new HashSet<>(jdbcClient.getAttributes(schemaTableName)));
        }
        return schemaTableNameAttributes;
    }

    private boolean checkIfSchemaTableNameCanBeAddedToTheOrdering(List<SchemaTableName> existingOrdering, SchemaTableName toBeAdded)
    {
        if (existingOrdering.size() == 0) {
            return true;
        }
        HashSet<String> union = new HashSet<>();
        for (SchemaTableName schemaTableName : existingOrdering) {
            union.addAll(schemaTableNameAttributes.get(schemaTableName));
        }
        HashSet<String> toBeAddedAttributes = schemaTableNameAttributes.get(toBeAdded);
        boolean canBeAdded = union.stream().anyMatch(toBeAddedAttributes::contains);
        if (Switches.DEBUG && log.isDebugEnabled()) {
            log.debug(String.format("existingOrdering: %s\ttoBeAdded: %s\tcanBeAdded: %s", existingOrdering, toBeAdded, canBeAdded));
        }
        return canBeAdded;
    }

    private JoinOrdering findTheMinimumExecutionCostOrdering(List<JoinOrdering> joinOrderings)
    {
        checkArgument(joinOrderings.size() > 0, "Given join orderings is empty");
        searchedPlan = new HashMap<>();
        float minimumAverageTime = Float.MAX_VALUE;
        JoinOrdering minimumExecutionCostOrdering = null;
        for (JoinOrdering joinOrdering : joinOrderings) {
            long executionTime = 0;
            for (int i = 0; i < executionTimes; ++i) {
                Plan plan = createPhysicalPlanFromJoinOrdering(joinOrdering, null);
                ExecutionNormal executionNormal = new ExecutionNormal(plan.getRoot());
                long currentTime = System.nanoTime();
                executionNormal.evalForBenchmark();
                executionTime += (System.nanoTime() - currentTime);
            }
            float averageTime = executionTime / (float) executionTimes;
            searchedPlan.put(joinOrdering, averageTime);
            if (averageTime < minimumAverageTime) {
                minimumAverageTime = averageTime;
                minimumExecutionCostOrdering = joinOrdering;
            }
        }
        checkState(minimumExecutionCostOrdering != null, "minimumExecutionCostOrdering cannot be null");
        builder = builder.searchedPlan(searchedPlan)
                .optimalJoinOrdering(minimumExecutionCostOrdering)
                .cost(minimumAverageTime);
        return minimumExecutionCostOrdering;
    }

    private void modifyPlanBasedOnOptimalJoinOrdering(PlanNode root, JoinOrdering optimalJoinOrdering)
    {
        List<SchemaTableName> schemaTableNameList = optimalJoinOrdering.getSchemaTableNameList();
        List<PlanNode> tableNodes = gatherTableScanNodes(root);
        HashMap<SchemaTableName, TableNode> schemaTableName2TableNodes = new HashMap<>();
        for (PlanNode node : tableNodes) {
            TableNode tableNode = (TableNode) node;
            schemaTableName2TableNodes.put(tableNode.getSchemaTableName(), tableNode);
        }
        List<SchemaTableName> schemaTableNameListReverse = new ArrayList<>(schemaTableNameList);
        Collections.reverse(schemaTableNameListReverse);
        PlanNode nodePtr = root;
        // Assume left-deep plan structure
        for (int i = 0; i < schemaTableNameListReverse.size() - 1; i++) {
            checkState(nodePtr.getNodeType() == OptType.join, "the given plan is not left-deep");
            JoinNode joinNodePtr = (JoinNode) nodePtr;
            joinNodePtr.setRight(schemaTableName2TableNodes.get(schemaTableNameListReverse.get(i)));
            if (i == schemaTableNameListReverse.size() - 2) {
                joinNodePtr.setLeft(schemaTableName2TableNodes.get(schemaTableNameListReverse.get(i + 1)));
            }
            nodePtr = joinNodePtr.getLeft();
        }
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        return node.isRoot() &&
                node.getNodeType() == OptType.join && // Root node of the given plan has to be a join
                (((JoinNode) node).getLeft().getNodeType() == OptType.join ||
                        (((JoinNode) node).getLeft().getNodeType() == OptType.table && ((JoinNode) node).getRight().getNodeType() == OptType.table)) && // The given plan is a left-deep plan
                !TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.isAssignableFrom(node.getOperator().getClass()); // enforce the rule cannot be applied to TTJ-family
    }

    @Override
    public RuleType getRuleType()
    {
        return RuleType.AS_A_WHOLE;
    }
}
