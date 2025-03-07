package org.zhu45.treetracker.relational.planner.rule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeHeightProvider;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.getLeftMostNode;
import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.TableScanVisitor.gatherTableScanNodes;

/**
 * We test the interaction of among rules.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRuleInteraction
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestRuleInteraction";
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBaseTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    /**
     * We test the interaction of two rules FindOptimalJoinTree and DisableNoGoodList
     */
    @Test
    public void testBothRulesCanBeApplied()
            throws JsonProcessingException
    {
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        // TODO: We probably want to implement some checking facility to ensure the order of input rules is correct, e.g.,
        //  in this case, DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee should be after FindOptimalJoinTree.
        //  We could maintain a dependency graph and ensure that input rules is a valid topological sort of the nodes (rules) in
        //  the graph given the first node in the sort is the first rule of the input rules.
        base.updateRules(List.of(new FindOptimalJoinTree(new JoinTreeHeightProvider()),
                new DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee(RuleType.AS_A_WHOLE)));
        JoinOrdering joinOrdering = new JoinOrdering(gatherTableScanNodes(pair.getLeft().getRoot()).stream().map(node ->
                ((TableNode) node).getSchemaTableName()).collect(Collectors.toList()));
        Pair<Plan, List<Operator>> plan = base.createPhysicalPlanFromJoinOrdering(joinOrdering);
        base.testPhysicalPlanExecution(plan);
        Operator leftMostOperator = getLeftMostNode(plan.getLeft().getRoot()).getOperator();
        assertEquals(TupleBasedTableScanOperator.class, leftMostOperator.getClass());
        assertNotNull(plan.getLeft().getPlanStatistics().getOptimalJoinTree());
        assertNotNull(plan.getLeft().getPlanStatistics().getOptimalJoinOrdering());
        assertNotNull(plan.getLeft().getPlanStatistics().getSearchedJoinTrees());
        assertNotEquals(0.0, plan.getLeft().getPlanStatistics().getCost());
        assertEquals(2, plan.getLeft().getPlanStatistics().getRuleStatisticsList().size());
        for (RuleStatistics ruleStatistics : plan.getLeft().getPlanStatistics().getRuleStatisticsList()) {
            if (ruleStatistics.getRuleName().equals(DisableNoGoodListWithoutViolatingTTJTheoreticalGuarantee.class.getName())) {
                assertNotNull(ruleStatistics.getDisabledTTJScan());
                assertEquals(1, ruleStatistics.getDisabledTTJScan().size());
            }
        }
        ObjectMapper objectMapper = new ObjectMapper();
        String jsonOutput = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(plan.getLeft().getPlanStatistics());
        assertTrue(jsonOutput.contains("disabledTTJScan"));
        List<PlanNode> nodes = gatherTableScanNodes(plan.getLeft().getRoot());
        for (PlanNode node : nodes) {
            assertNotNull(node.getOperator().getPlanBuildContext());
        }
    }
}
