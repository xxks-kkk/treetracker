package org.zhu45.treetracker.relational.operator;

import org.apache.commons.collections4.MultiSet;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.columnCompare;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.rowCompare;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedLeftSemiHashJoinIntOperator
{
    private Logger traceLogger = LogManager.getLogger(TestTupleBasedLeftSemiHashJoinIntOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestTupleBasedLeftSemiHashJoinIntOperator";

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestTupleBasedLeftSemiHashJoinIntOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedJoinOperator.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(TupleBasedLeftSemiHashJoinIntOperator.class))));
    }

    private Stream<Arguments> testTupleBasedLeftSemiHashJoinIntOperatorDataProvider()
    {
        TestTupleBasedLeftSemiHashJoinOperator.TestTupleBasedLeftSemiHashJoinOperatorTestCases cases = new TestTupleBasedLeftSemiHashJoinOperator.TestTupleBasedLeftSemiHashJoinOperatorTestCases(base);
        return Stream.of(Arguments.of(cases.testCase2()));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedLeftSemiHashJoinIntOperatorDataProvider")
    public void testTupleBasedLeftSemiHashJoinIntOperator(Triple<Plan, List<Operator>, MultiSet<Row>> triple)
    {
        try {
            ExecutionNormal executionNormal = new ExecutionNormal(triple.getLeft().getRoot());
            MultiSet<Row> actual = executionNormal.eval();
            boolean rowCompareRes = rowCompare(triple.getRight(), actual);
            boolean columnCompareRes = columnCompare(triple.getRight(), actual);
            assertTrue(rowCompareRes && columnCompareRes);
        }
        finally {
            triple.getMiddle().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
