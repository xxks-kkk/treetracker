package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestStatisticsInformationPrinter
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestStatisticsInformationPrinter";
    private StatisticsInformationPrinter printer;
    private TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases;

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
                Collections.emptyList(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBaseTreeTrackerOneBetaTableScanOperator.class),
                        Optional.of(TupleBaseTreeTrackerOneBetaHashTableOperator.class))));
        cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        printer = new StatisticsInformationPrinter();
    }

    @Test
    public void testStatisticsInformationPrinter()
    {
        Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertTrue(statistics.contains("TTJ operator (2: join2): \n" +
                "numberOfNoGoodTuples: 2"));
        assertTrue(statistics.contains("Table scan operator (0: caseone_T): \n" +
                "fetchingTuplesTime (ms): 0\n" +
                "RecordTupleSourceClazzName: org.zhu45.treetracker.jdbc.RecordObjectTupleSourceProvider\n" +
                "numberOfNoGoodTuples: 2\n" +
                "numberOfNoGoodTuplesFiltered: 0"));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
