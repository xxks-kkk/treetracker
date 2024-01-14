package org.zhu45.treetracker.relational.planner.cost;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.jdbc.PostgreSqlClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TestTTJAggregateStatisticsInformationWithTTJV2Operator;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode.getTableNode;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinTreeCostEstProvider
{
    private static final Logger traceLogger = LogManager.getLogger(TestJoinTreeCostEstProvider.class.getName());

    private static final String naturalJoinTable = "TestJoinTreeCostEstProvider";
    private TestingPhysicalPlanBase base;

    private MultiwayJoinNode infoTypeNode;
    private MultiwayJoinNode movieInfoIdxNode;
    private MultiwayJoinNode movieCompaniesNode;
    private MultiwayJoinNode companyTypeNode;
    private MultiwayJoinNode titleNode;
    private MultiwayJoinOrderedGraph joinTree;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestJoinTreeCostEstProvider.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(JoinTreeCostEstProvider.class.getName(), Level.DEBUG);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.empty());
        setupTestTables(base);
    }

    private void setupTestTables(TestingPhysicalPlanBase base)
    {
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        List<List<RelationalValue>> relation3Vals = new ArrayList<>(Arrays.asList(
                List.of(IntegerValue.of(0), IntegerValue.of(0), IntegerValue.of(0))
        ));
        List<List<RelationalValue>> relation2Vals = new ArrayList<>(Arrays.asList(
                List.of(IntegerValue.of(0), IntegerValue.of(0))
        ));
        List<List<RelationalValue>> relation1Val = new ArrayList<>(Arrays.asList(
                List.of(IntegerValue.of(0))
        ));

        String tableNamePrefix = "JTCEst";
        SchemaTableName infoTypeSchemaTableName = new SchemaTableName(base.getDatabase().getSchemaName(),
                tableNamePrefix + "info_type");
        SchemaTableName movieInfoIdxSchemaTableName = new SchemaTableName(base.getDatabase().getSchemaName(),
                tableNamePrefix + "movie_info_idx");
        SchemaTableName movieCompaniesSchemaTableName = new SchemaTableName(base.getDatabase().getSchemaName(),
                tableNamePrefix + "movie_companies");
        SchemaTableName companyTypeSchemaTableName = new SchemaTableName(base.getDatabase().getSchemaName(),
                tableNamePrefix + "company_type");
        SchemaTableName titleSchemaTableName = new SchemaTableName(base.getDatabase().getSchemaName(),
                tableNamePrefix + "title");

        if (jdbcClient.getTableHandle(infoTypeSchemaTableName) == null) {
            jdbcClient.ingestRelation(
                    infoTypeSchemaTableName.getSchemaName(),
                    infoTypeSchemaTableName.getTableName(),
                    new ArrayList<>(List.of("info_type_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relation1Val);
        }
        if (jdbcClient.getTableHandle(movieInfoIdxSchemaTableName) == null) {
            jdbcClient.ingestRelation(
                    movieInfoIdxSchemaTableName.getSchemaName(),
                    movieInfoIdxSchemaTableName.getTableName(),
                    new ArrayList<>(List.of("movie_id", "info_type_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relation2Vals);
        }
        if (jdbcClient.getTableHandle(movieCompaniesSchemaTableName) == null) {
            jdbcClient.ingestRelation(
                    movieCompaniesSchemaTableName.getSchemaName(),
                    movieCompaniesSchemaTableName.getTableName(),
                    new ArrayList<>(List.of("movie_id", "company_id", "company_type_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER)),
                    relation3Vals);
        }
        if (jdbcClient.getTableHandle(companyTypeSchemaTableName) == null) {
            jdbcClient.ingestRelation(
                    companyTypeSchemaTableName.getSchemaName(),
                    companyTypeSchemaTableName.getTableName(),
                    new ArrayList<>(List.of("company_type_id")),
                    new ArrayList<>(List.of(INTEGER)),
                    relation1Val);
        }
        if (jdbcClient.getTableHandle(titleSchemaTableName) == null) {
            jdbcClient.ingestRelation(
                    titleSchemaTableName.getSchemaName(),
                    titleSchemaTableName.getTableName(),
                    new ArrayList<>(List.of("movie_id", "kind_id")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relation2Vals);
        }

        infoTypeNode = getTableNode(infoTypeSchemaTableName, base.getDatabase().getJdbcClient());
        movieInfoIdxNode = getTableNode(movieInfoIdxSchemaTableName, base.getDatabase().getJdbcClient());
        movieCompaniesNode = getTableNode(movieCompaniesSchemaTableName, base.getDatabase().getJdbcClient());
        companyTypeNode = getTableNode(companyTypeSchemaTableName, base.getDatabase().getJdbcClient());
        titleNode = getTableNode(titleSchemaTableName, base.getDatabase().getJdbcClient());

        List<MultiwayJoinNode> traversalList = List.of(infoTypeNode, movieInfoIdxNode, movieCompaniesNode, companyTypeNode, titleNode);

        joinTree = new MultiwayJoinOrderedGraph(infoTypeNode, Arrays.asList(
                asDirectedEdge(infoTypeNode, movieInfoIdxNode),
                asDirectedEdge(movieInfoIdxNode, movieCompaniesNode),
                asDirectedEdge(movieCompaniesNode, companyTypeNode),
                asDirectedEdge(movieCompaniesNode, titleNode)), traversalList);
    }

    @Test
    public void testSQLGenerated()
            throws SQLException
    {
        PostgreSqlClient postgresJdbcClient = (PostgreSqlClient) JdbcSupplier.postgresJdbcClientSupplier.get();
        JoinTreeCostEstProvider.CostEstimateQueryBuilder costEstimateQueryBuilder =
                new JoinTreeCostEstProvider.CostEstimateQueryBuilder(postgresJdbcClient.getIdentifierQuote(), false);
        try (Connection connection = postgresJdbcClient.getConnection()) {
            for (MultiwayJoinNode node : joinTree.getTraversalList()) {
                if (node.equals(infoTypeNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, movieInfoIdxNode, List.of(), joinTree)) {
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT distinct(info_type_id) FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\" WHERE (info_type_id) NOT IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" EXCEPT ALL SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" EXCEPT ALL SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (company_type_id) NOT IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\") EXCEPT ALL SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")))",
                                preparedStatement.toString());
                        assertTrue(preparedStatement.execute());
                    }
                }
                else if (node.equals(movieInfoIdxNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, movieCompaniesNode, List.of(), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" EXCEPT ALL SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (company_type_id) NOT IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\") EXCEPT ALL SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\"))",
                                preparedStatement.toString());
                    }
                }
                else if (node.equals(movieCompaniesNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, companyTypeNode, List.of(), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,company_id,company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (company_type_id) NOT IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")",
                                preparedStatement.toString());
                    }
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, titleNode, List.of(companyTypeNode), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,company_id,company_type_id FROM (SELECT movie_id,company_id,company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" EXCEPT ALL SELECT movie_id,company_id,company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (company_type_id) NOT IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")) AS alias0 WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\"natural join\"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\")AND (movie_id) NOT IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")",
                                preparedStatement.toString());
                    }
                }
            }
        }
    }

    public static class TestCostModelCases
            implements TestCases
    {
        TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases cases;
        String schemaName;
        JdbcClient jdbcClient;
        TestingPhysicalPlanBase base;

        public TestCostModelCases(TestingPhysicalPlanBase base)
        {
            cases = new TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases(base);
            schemaName = base.getDatabase().getSchemaName();
            jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> case1()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(test.getLeft(), test.getMiddle(), 8);
        }

        public Triple<Plan, List<Operator>, Integer> case2()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.physicalPlan2ForTestEstimateCostOfTTJ();
            return Triple.of(test.getLeft(), test.getMiddle(), 3);
        }

        public Triple<Plan, List<Operator>, Integer> case3()
        {
            String relationS = this.getClass().getSimpleName() + "_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            List<String> attributeS = List.of("x", "y", "z");
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(3)),
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2), IntegerValue.of(1))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationS,
                        attributeS,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER, INTEGER)),
                        relationValS);
            }
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributeS, new MultiwayJoinDomain());

            String relationR = this.getClass().getSimpleName() + "_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            List<String> attributesR = List.of("y", "z");
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(2), new IntegerValue(1))));
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
            List<String> attributesB = List.of("z");
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationB,
                        attributesB,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValB);
            }
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributesB, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeS, nodeB),
                    asEdge(nodeS, nodeR))), nodeS);

            Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                    Optional.empty(),
                    Optional.empty());
            return Triple.of(pair.getLeft(), pair.getRight(), 1);
        }

        public Triple<Plan, List<Operator>, Integer> case4()
        {
            String relationPrefix = this.getClass().getSimpleName() + "_case4";
            String relationT = relationPrefix + "_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            List<String> attributeT = List.of("a", "b");
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2)),
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(4)),
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(6))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationT,
                        attributeT,
                        new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                        relationValT);
            }
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, attributeT, new MultiwayJoinDomain());

            String relationS = relationPrefix + "_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            List<String> attributesS = List.of("a");
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        Arrays.asList(new IntegerValue(3))));
                jdbcClient.ingestRelation(
                        schemaName,
                        relationS,
                        attributesS,
                        new ArrayList<>(Arrays.asList(INTEGER)),
                        relationValS);
            }
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributesS, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeT, nodeS))), nodeT);

            Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                    Optional.empty(),
                    Optional.empty());
            return Triple.of(pair.getLeft(), pair.getRight(), 1);
        }
    }

    private Object[][] testCostModelDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestCostModelCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testCostModelDataProvider")
    public void testEstimateCostOfTTJ(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .build();
        JoinTreeCostEstProvider costEstProvider = new JoinTreeCostEstProvider(config);
        PlanPrinter planPrinter = new PlanPrinter(triple.getLeft().getRoot());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(planPrinter.toText(0));
        }
        JoinTreeCostReturn costReturn = costEstProvider.getCost(new JoinOrdering(getSchemaTableNames(triple.getLeft().getRoot())),
                triple.getLeft().getRoot().getOperator().getPlanBuildContext().getOrderedGraph(),
                triple.getLeft().getRoot().getOperator().getPlanBuildContext());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(costReturn.getCost());
        }
        assertEquals(expectedValue, costReturn.getCost());
    }

    public static class TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCases
            implements TestCases
    {
        TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases cases;

        public TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCases(TestingPhysicalPlanBase base)
        {
            cases = new TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases(base);
        }

        public Triple<Plan, List<Operator>, Integer> case1()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(test.getLeft(), test.getMiddle(), 13);
        }

        public Triple<Plan, List<Operator>, Integer> case2()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.physicalPlan2ForTestEstimateCostOfTTJ();
            return Triple.of(test.getLeft(), test.getMiddle(), 7);
        }
    }

    private Object[][] TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCasesDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCases.class)));
    }

    @ParameterizedTest
    @MethodSource("TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCasesDataProvider")
    public void testTestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
                .build();
        JoinTreeCostEstProvider costEstProvider = new JoinTreeCostEstProvider(config);
        PlanPrinter planPrinter = new PlanPrinter(triple.getLeft().getRoot());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(planPrinter.toText(0));
        }
        JoinTreeCostReturn costReturn = costEstProvider.getCost(new JoinOrdering(getSchemaTableNames(triple.getLeft().getRoot())),
                triple.getLeft().getRoot().getOperator().getPlanBuildContext().getOrderedGraph(),
                triple.getLeft().getRoot().getOperator().getPlanBuildContext());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(costReturn.getCost());
        }
        assertEquals(expectedValue, costReturn.getCost());
    }

    public static class TestCostModelIncludeInnerRelationCleanStateSizeCases
            implements TestCases
    {
        TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases cases;

        public TestCostModelIncludeInnerRelationCleanStateSizeCases(TestingPhysicalPlanBase base)
        {
            cases = new TestTTJAggregateStatisticsInformationWithTTJV2Operator.TestEstimateCostOfTTJCases(base);
        }

        public Triple<Plan, List<Operator>, Integer> case1()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.physicalPlanForTestEstimateCostOfTTJ();
            return Triple.of(test.getLeft(), test.getMiddle(), 17);
        }

        public Triple<Plan, List<Operator>, Integer> case2()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.physicalPlan2ForTestEstimateCostOfTTJ();
            return Triple.of(test.getLeft(), test.getMiddle(), 10);
        }
    }

    private Object[][] TestCostModelIncludeInnerRelationCleanStateSizeDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestCostModelIncludeInnerRelationCleanStateSizeCases.class)));
    }

    @ParameterizedTest
    @MethodSource("TestCostModelIncludeInnerRelationCleanStateSizeDataProvider")
    public void testTestCostModelIncludeInnerRelationCleanStateSize(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
                .includeInnerRelationSize(true)
                .build();
        JoinTreeCostEstProvider costEstProvider = new JoinTreeCostEstProvider(config);
        PlanPrinter planPrinter = new PlanPrinter(triple.getLeft().getRoot());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(planPrinter.toText(0));
        }
        JoinTreeCostReturn costReturn = costEstProvider.getCost(new JoinOrdering(getSchemaTableNames(triple.getLeft().getRoot())),
                triple.getLeft().getRoot().getOperator().getPlanBuildContext().getOrderedGraph(),
                triple.getLeft().getRoot().getOperator().getPlanBuildContext());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(costReturn.getCost());
        }
        assertEquals(expectedValue, costReturn.getCost());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
