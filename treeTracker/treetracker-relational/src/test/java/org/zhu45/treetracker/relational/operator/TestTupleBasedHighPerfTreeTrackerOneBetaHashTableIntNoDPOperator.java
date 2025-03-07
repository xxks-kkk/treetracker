package org.zhu45.treetracker.relational.operator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collections;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.getLeftMostNode;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator
{
    private static final String naturalJoinTable = "TestOneBetaHashTableIntNoDPOperator";

    private TestingPhysicalPlanBase base;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class.getName(), Level.TRACE);
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
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class))));
    }

    @Test
    public void testDeletionPropagationIsDisabled()
    {
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        var pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseNine();
        base.testPhysicalPlanExecution(pair);
        // Due to disable of deletion propagation, T(1) is not added to the no-good list.
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
