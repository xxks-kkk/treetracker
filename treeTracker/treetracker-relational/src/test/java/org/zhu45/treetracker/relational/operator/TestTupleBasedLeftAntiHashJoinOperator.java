package org.zhu45.treetracker.relational.operator;

import org.apache.commons.collections4.MultiSet;
import org.apache.commons.collections4.multiset.HashMultiSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.JoinValueContainerKey;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.columnCompare;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.rowCompare;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedLeftAntiHashJoinOperator
{
    private Logger traceLogger = LogManager.getLogger(TestTupleBasedLeftAntiHashJoinOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestTupleBasedLeftAntiHashJoinOperator";

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestTupleBasedLeftAntiHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(JoinValueContainerKey.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(TupleBasedLeftAntiHashJoinOperator.class))));
    }

    public static class TestTupleBasedLeftAntiHashJoinOperatorTestCases
            implements TestCases
    {
        private String schemaName;
        private JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestTupleBasedLeftAntiHashJoinOperatorTestCases(TestingPhysicalPlanBase base)
        {
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Triple<Plan, List<Operator>, MultiSet<ObjectRow>> testCase1()
        {
            JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
            String schemaName = base.getDatabase().getSchemaName();

            String relationT = this.getClass().getSimpleName() + "_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("red"), new StringValue("l")),
                        Arrays.asList(new StringValue("blue"), new StringValue("l")),
                        Arrays.asList(new StringValue("red"), new StringValue("x")),
                        Arrays.asList(new StringValue("yellow"), new StringValue("y"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationT,
                        new ArrayList<>(Arrays.asList("color", "letter2")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                        relationValT);
            }
            MultiwayJoinDomain domainT = new MultiwayJoinDomain();
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

            String relationS = this.getClass().getSimpleName() + "_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("red"), new StringValue("a"), new StringValue("l"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationS,
                        new ArrayList<>(Arrays.asList("color", "letter1", "letter2")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                        relationValS);
            }
            MultiwayJoinDomain domainS = new MultiwayJoinDomain();
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

            String relationR = this.getClass().getSimpleName() + "_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("yellow")),
                        Arrays.asList(new StringValue("blue"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR,
                        new ArrayList<>(Arrays.asList("color")),
                        new ArrayList<>(Arrays.asList(VARCHAR)),
                        relationValR);
            }
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, null);

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeT, nodeS),
                    asEdge(nodeS, nodeR))), nodeT);

            Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                            new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameR))),
                    Optional.empty());
            MultiSet<ObjectRow> res = new HashMultiSet<>();
            res.add(new ObjectRow(new ArrayList<>(Arrays.asList("color", "letter2")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                    Arrays.asList(new StringValue("red"), new StringValue("x"))));
            return Triple.of(pair.getLeft(), pair.getRight(), res);
        }
    }

    private Object[][] testTupleBasedLeftAntiHashJoinOperatorDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestTupleBasedLeftAntiHashJoinOperatorTestCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedLeftAntiHashJoinOperatorDataProvider")
    public void testTupleBasedLeftAntiHashJoinOperator(Triple<Plan, List<Operator>, MultiSet<Row>> triple)
    {
        try {
            ExecutionNormal executionNormal = new ExecutionNormal(triple.getLeft().getRoot());
            MultiSet<Row> actual = executionNormal.eval();
            boolean rowCompareRes = rowCompare(triple.getRight(), actual);
            boolean columnCompareRes = columnCompare(triple.getRight(), actual);
            assertTrue(rowCompareRes && columnCompareRes);
        }
        finally {
            triple.getMiddle().forEach(Operator::close);
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
