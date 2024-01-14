package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFullReducerOperatorWithBloomSemiJoin
{
    private Logger traceLogger = LogManager.getLogger(TestFullReducerOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestFullReducerOperatorWithBloomSemiJoin";
    private StatisticsInformationPrinter printer;
    private TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(FullReducerOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedLeftSemiBloomJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TestFullReducerOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TestFullReducerOperatorWithBloomSemiJoin.class.getName(), Level.DEBUG);
        }

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(TupleBasedHashJoinOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    public static class TestFullReducerOperatorWithBloomSemiJoinTestCases
            implements TestCases
    {
        private TestFullReducerOperator.TestFullReducerTestGraphs testFullReducerTestGraphs;
        private PlanBuildContext.Builder builder;
        private TestingPhysicalPlanBase base;

        public TestFullReducerOperatorWithBloomSemiJoinTestCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
            this.testFullReducerTestGraphs = new TestFullReducerOperator.TestFullReducerTestGraphs(base);
            this.builder = builder()
                    .setRules(List.of(new AttachFullReducer()))
                    .setPlanNodeIdAllocator(base.getIdAllocator())
                    .setOperatorMap(base.getOperatorMap())
                    .setSemiJoinClazz(TupleBasedLeftSemiBloomJoinOperator.class);
        }

        public Triple<Plan, List<Operator>, List<String>> physicalPlan2ForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = testFullReducerTestGraphs.graph2ForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of(
                    "multiwaycomplex.TestFullReducerTestGraphs_R3 [1]",
                    "multiwaycomplex.TestFullReducerTestGraphs_R4 [1]",
                    "multiwaycomplex.TestFullReducerTestGraphs_R1 [1]",
                    "multiwaycomplex.TestFullReducerTestGraphs_R2 [3]",
                    "estimateCostOfFullReducer: 28"
            ));
        }

        public Triple<Plan, List<Operator>, List<String>> physicalPlan3ForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = testFullReducerTestGraphs.graph3ForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(
                    new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of(
                    "multiwaycomplex.physicalPlan3ForTestFullReducerOperator_R1 [2]"
            ));
        }

        public Triple<Plan, List<Operator>, List<String>> physicalPlan4ForTestFullReducerOperator()
        {
            Pair<MultiwayJoinOrderedGraph, List<SchemaTableName>> pair = testFullReducerTestGraphs.graph4ForTestFullReducerOperator();
            PlanBuildContext context = builder.setOrderedGraph(pair.getKey()).build();
            Pair<Plan, List<Operator>> planPair = base.createFixedPhysicalPlanFromQueryGraph(context, Optional.of(
                    new LinkedList<>(pair.getValue())));
            return Triple.of(planPair.getLeft(), planPair.getRight(), List.of());
        }
    }

    private Object[][] testFullReducerOperatorWithBloomSemiJoinDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestFullReducerOperatorWithBloomSemiJoinTestCases.class)));
    }

    @DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
    @ParameterizedTest
    @MethodSource("testFullReducerOperatorWithBloomSemiJoinDataProvider")
    public void testFullReducerOperatorWithBloomSemiJoin(Triple<Plan, List<Operator>, List<String>> triple)
    {
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        Operator rootOperator = triple.getLeft().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("statistics:\n" + statistics);
        }
        triple.getRight().forEach(expectedMsg -> {
            if (traceLogger.isDebugEnabled()) {
                traceLogger.debug("expectedMsg:\n" + expectedMsg);
            }
            assertTrue(statistics.contains(expectedMsg));
        });
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
