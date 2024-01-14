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
public class TestTupleBasedLeftSemiHashJoinOperator
{
    private Logger traceLogger = LogManager.getLogger(TestTupleBasedLeftSemiHashJoinOperator.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestTupleBasedLeftSemiHashJoinOperator";

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TupleBasedLeftSemiHashJoinOperator.class.getName(), Level.TRACE);
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
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(TupleBasedLeftSemiHashJoinOperator.class))));
    }

    public static class TestTupleBasedLeftSemiHashJoinOperatorTestCases
            implements TestCases
    {
        private String schemaName;
        private JdbcClient jdbcClient;
        private TestingPhysicalPlanBase base;

        public TestTupleBasedLeftSemiHashJoinOperatorTestCases(TestingPhysicalPlanBase base)
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

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeT, nodeS))), nodeT);

            Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                            new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS))),
                    Optional.empty());
            MultiSet<ObjectRow> res = new HashMultiSet<>();
            res.add(new ObjectRow(new ArrayList<>(Arrays.asList("color", "letter2")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                    Arrays.asList(new StringValue("red"), new StringValue("l"))));
            return Triple.of(pair.getLeft(), pair.getRight(), res);
        }

        public Triple<Plan, List<Operator>, MultiSet<ObjectRow>> testCase2()
        {
            JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
            String schemaName = base.getDatabase().getSchemaName();

            String relationR2 = this.getClass().getSimpleName() + "_R2";
            SchemaTableName schemaTableNameR2 = new SchemaTableName(schemaName, relationR2);
            if (jdbcClient.getTableHandle(schemaTableNameR2) == null) {
                List<List<RelationalValue>> relationValR2 = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("1"), new StringValue("10"), new StringValue("100")),
                        Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("100")),
                        Arrays.asList(new StringValue("3"), new StringValue("10"), new StringValue("300")),
                        Arrays.asList(new StringValue("1"), new StringValue("40"), new StringValue("300")),
                        Arrays.asList(new StringValue("2"), new StringValue("30"), new StringValue("200"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR2,
                        new ArrayList<>(Arrays.asList("A1", "A2", "A3")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                        relationValR2);
            }
            MultiwayJoinDomain domainR2 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR2 = new MultiwayJoinNode(schemaTableNameR2, domainR2);

            String relationR4 = this.getClass().getSimpleName() + "_R4";
            SchemaTableName schemaTableNameR4 = new SchemaTableName(schemaName, relationR4);
            if (jdbcClient.getTableHandle(schemaTableNameR4) == null) {
                List<List<RelationalValue>> relationValR4 = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new StringValue("1"), new StringValue("10"), new StringValue("1000")),
                        Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("1000")),
                        Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("2000")),
                        Arrays.asList(new StringValue("2"), new StringValue("20"), new StringValue("2000"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR4,
                        new ArrayList<>(Arrays.asList("A1", "A2", "A4")),
                        new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                        relationValR4);
            }
            MultiwayJoinDomain domainR4 = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR4 = new MultiwayJoinNode(schemaTableNameR4, domainR4);

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeR2, nodeR4))), nodeR2);

            Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                            new LinkedList<>(Arrays.asList(schemaTableNameR2, schemaTableNameR4))),
                    Optional.empty());
            MultiSet<ObjectRow> res = new HashMultiSet<>();
            res.addAll(Arrays.asList(
                    new ObjectRow(new ArrayList<>(Arrays.asList("A1", "A2", "A3")),
                            new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                            Arrays.asList(new StringValue("1"), new StringValue("10"), new StringValue("100"))),
                    new ObjectRow(new ArrayList<>(Arrays.asList("A1", "A2", "A3")),
                            new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                            Arrays.asList(new StringValue("1"), new StringValue("20"), new StringValue("100")))));
            return Triple.of(pair.getLeft(), pair.getRight(), res);
        }
    }

    private Object[][] testTupleBasedLeftSemiHashJoinOperatorDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestTupleBasedLeftSemiHashJoinOperatorTestCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedLeftSemiHashJoinOperatorDataProvider")
    public void testTupleBasedLeftSemiHashJoinOperator(Triple<Plan, List<Operator>, MultiSet<Row>> triple)
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
