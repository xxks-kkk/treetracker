package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.TestTupleBaseTreeTrackerOneBetaHashTableOperator.testLeftDeepQueryPlansTestCasesOnly;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildAllTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.getLeftMostNode;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator
{
    private static final String naturalJoinTable = "TestTupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator";

    private TestingPhysicalPlanBase base;
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
            Configurator.setAllLevels(TestingMultiwayJoinDatabaseComplex.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(ExecutionNormal.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    private Object[][] testTupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperatorTestCasesDataProvider()
    {
        return twoDlistTo2DArray(buildAllTestCases(base));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperatorTestCasesDataProvider")
    public void testTupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator(Pair<Plan, List<Operator>> pair)
    {
        testLeftDeepQueryPlansTestCasesOnly(pair, base);
    }

    @Test
    public void testDeletionPropagationIsDisabled()
    {
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        var pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseEight();
        base.testPhysicalPlanExecution(pair);
        // Due to disable of deletion propagation, T(red) is not added to the no-good list.
        assertEquals(0, getLeftMostNode(pair.getLeft().getRoot()).getOperator().getStatisticsInformation().getNumberOfNoGoodTuples());
    }

    @Test
    public void testDeletionPropagationIsDisabled2()
    {
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        var pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseTen();
        base.testPhysicalPlanExecution(pair);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
