package org.zhu45.treetracker.benchmark;

import lombok.Data;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.statistics.CostModel;
import org.zhu45.treetracker.relational.statistics.CostModel4Weak;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase.charType;
import static org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex.schemaName;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

public class TestCostModelFactory
{
    private static Stream<Arguments> testCostModel4WeakProvider()
    {
        return Stream.of(
                Arguments.of(new TestInput(TestQueries.TestQuery1, JoinOperator.TTJHP, 22)),
                Arguments.of(new TestInput(TestQueries.TestQuery1, JoinOperator.Yannakakis, 40)),
                Arguments.of(new TestInput(TestQueries.TestQuery2, JoinOperator.TTJHP, 11)),
                Arguments.of(new TestInput(TestQueries.TestQuery2, JoinOperator.Yannakakis, 25))
        );
    }

    @ParameterizedTest
    @MethodSource({"testCostModel4WeakProvider"})
    public void testCostModel4Weak(TestInput testInput)
    {
        CostModelFactory factory = new CostModelFactory(testInput.query, CostModel4Weak.class, postgresJdbcClientSupplier.get());
        CostModel costModel = factory.get();
        assertEquals(0, BigDecimal.valueOf(testInput.expectedCost).compareTo(costModel.getCost(testInput.joinOperator)));
    }

    @Data
    private static class TestInput
    {
        QueryEnum query;
        JoinOperator joinOperator;
        long expectedCost;

        public TestInput(QueryEnum query, JoinOperator joinOperator, long expectedCost)
        {
            this.query = query;
            this.joinOperator = joinOperator;
            this.expectedCost = expectedCost;
        }
    }

    public enum TestQueries
            implements QueryEnum
    {
        TestQuery1("TestQuery1"),
        TestQuery2("TestQuery2");

        private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

        static {
            for (TestQueries c : values()) {
                switch (c) {
                    case TestQuery1:
                        map.put(c.val, TestQuery1.class);
                        break;
                    case TestQuery2:
                        map.put(c.val, TestQuery2.class);
                        break;
                }
            }
        }

        private final String val;

        private TestQueries(String s)
        {
            val = s;
        }

        @Override
        public Class<? extends Query> getQueryClazz()
        {
            return map.get(val);
        }
    }

    private static class TestQuery1
            extends Query
    {
        public TestQuery1(JoinFragmentContext context)
        {
            super(context);
        }

        @Override
        protected Pair<Plan, List<Operator>> constructQuery()
        {
            String relationC = this.getClass().getSimpleName() + "_C";
            SchemaTableName schemaTableNameC = new SchemaTableName(schemaName, relationC);
            List<String> attributesC = List.of("y");
            if (jdbcClient.getTableHandle(schemaTableNameC) == null) {
                List<List<RelationalValue>> relationValC = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(1)),
                        Arrays.asList(new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(6)),
                        Arrays.asList(new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationC,
                        attributesC,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValC);
            }
            MultiwayJoinNode nodeC = new MultiwayJoinNode(schemaTableNameC, attributesC, new MultiwayJoinDomain());

            String relationR = this.getClass().getSimpleName() + "_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            List<String> attributesR = List.of("x", "y");
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(2), new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(3)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(6)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR,
                        attributesR,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValR);
            }
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributesR, new MultiwayJoinDomain());

            String relationB = this.getClass().getSimpleName() + "_B";
            SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
            List<String> attributesB = List.of("x", "y");
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(2), new IntegerValue(6)),
                        Arrays.asList(new IntegerValue(2), new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationB,
                        attributesB,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValB);
            }
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributesB, new MultiwayJoinDomain());

            String relationG = this.getClass().getSimpleName() + "_G";
            SchemaTableName schemaTableNameG = new SchemaTableName(schemaName, relationG);
            List<String> attributesG = List.of("y");
            if (jdbcClient.getTableHandle(schemaTableNameG) == null) {
                List<List<RelationalValue>> relationValG = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(5)),
                        Arrays.asList(new IntegerValue(5))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationG,
                        attributesG,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValG);
            }
            MultiwayJoinNode nodeG = new MultiwayJoinNode(schemaTableNameG, attributesG, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                    asEdge(nodeC, nodeR),
                    asEdge(nodeR, nodeB),
                    asEdge(nodeB, nodeG)), nodeC);

            return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        }
    }

    private static class TestQuery2
            extends Query
    {

        public TestQuery2(JoinFragmentContext context)
        {
            super(context);
        }

        @Override
        protected Pair<Plan, List<Operator>> constructQuery()
        {
            String relationT = "casefour_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                        Collections.singletonList(new StringValue(charType, "green")),
                        Collections.singletonList(new StringValue(charType, "green")),
                        Collections.singletonList(new StringValue(charType, "red"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationT,
                        new ArrayList<>(List.of("x")),
                        new ArrayList<>(List.of(charType)),
                        relationValT);
            }
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, new MultiwayJoinDomain());

            String relationS = "casefour_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        List.of(new StringValue(charType, "red"),
                                new StringValue(charType, "1"),
                                new StringValue(charType, "2")),
                        Arrays.asList(new StringValue(charType, "red"),
                                new StringValue(charType, "3"),
                                new StringValue(charType, "2"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationS,
                        new ArrayList<>(Arrays.asList("x", "y", "z")),
                        new ArrayList<>(Arrays.asList(charType, charType, charType)),
                        relationValS);
            }
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, new MultiwayJoinDomain());

            String relationB = "casefour_B";
            SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(
                        Collections.singletonList(new StringValue(charType, "2"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationB,
                        new ArrayList<>(List.of("z")),
                        new ArrayList<>(List.of(charType)),
                        relationValB);
            }
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, new MultiwayJoinDomain());

            String relationR = "casefour_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(
                        Arrays.asList(new StringValue(charType, "3"),
                                new StringValue(charType, "2"))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationR,
                        new ArrayList<>(Arrays.asList("y", "z")),
                        new ArrayList<>(Arrays.asList(charType, charType)),
                        relationValR);
            }
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                    asEdge(nodeT, nodeS),
                    asEdge(nodeS, nodeB),
                    asEdge(nodeS, nodeR)
            ), nodeT);
            return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        }
    }
}
