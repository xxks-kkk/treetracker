package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.TestTupleBaseTreeTrackerOneBetaHashTableOperator.testLeftDeepQueryPlansTestCasesOnly;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildAllTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedLIPHashJoinOperator
{
    private static final String naturalJoinTable = "TestTupleBasedLIPHashJoinOperator";
    private TestingPhysicalPlanBase base;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TupleBasedLIPHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedLIPTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.of(TupleBasedLIPTableScanOperator.class), Optional.of(TupleBasedLIPHashJoinOperator.class))));
    }

    private Object[][] testTupleBasedLIPHashJoinOperatorTestCasesDataProvider()
    {
        return twoDlistTo2DArray(buildAllTestCases(base));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedLIPHashJoinOperatorTestCasesDataProvider")
    public void testTupleBasedLIPHashJoinOperator(Pair<Plan, List<Operator>> pair)
    {
        testLeftDeepQueryPlansTestCasesOnly(pair, base);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
