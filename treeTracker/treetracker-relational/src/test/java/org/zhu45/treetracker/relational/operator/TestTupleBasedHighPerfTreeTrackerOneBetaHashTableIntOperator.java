package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.cost.TestJoinTreeCostEstProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator
{
    private Logger traceLogger = LogManager.getLogger(TestTupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestTupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator";
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    @Test
    public void testNumberOfR1Assignment()
    {
        TestTTJAggregateStatisticsInformation.TestNumberOfR1AssignmentCases cases = new TestTTJAggregateStatisticsInformation.TestNumberOfR1AssignmentCases(base);
        Triple<Plan, List<Operator>, Integer> triple = cases.physicalPlanForTestNumberOfR1Assignment();
        int expectedValue = triple.getRight();
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("expectedValue:\n" + expectedValue);
        }
        TTJIntAggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(triple);
        assertEquals(expectedValue, aggregateStatisticsInformation.getNumberOfR1Assignments());
    }

    private TTJIntAggregateStatisticsInformation testStatistics(Triple<Plan, List<Operator>, Integer> triple)
    {
        base.testPhysicalPlanExecution(Pair.of(triple.getLeft(), triple.getMiddle()));
        Operator rootOperator = triple.getLeft().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("statistics:\n" + statistics);
        }
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.TTJHP_INT)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        return (TTJIntAggregateStatisticsInformation) factory.get();
    }

    @Test
    public void testNumberOfNoGoodTuples()
    {
        List<Triple<Plan, List<Operator>, Integer>> tests = new ArrayList<>();
        TestJoinTreeCostEstProvider.TestCostModelCases cases = new TestJoinTreeCostEstProvider.TestCostModelCases(base);
        tests.add(cases.case1());
        // cases.case3() is not added because we only implement void updateNoGoodListMap(Row row, int id)
        // for SingleValueJavIntRowNoGoodListMap.
        tests.add(cases.case4());
        for (var triple : tests) {
            TTJIntAggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(triple);
            assertEquals((long) triple.getRight(), aggregateStatisticsInformation.getNumberOfDanglingTuples());
        }
    }
}
