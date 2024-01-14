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
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.jdbc.PostgreSqlClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinTreeCostEstYannakakisProvider
{
    private static final Logger traceLogger = LogManager.getLogger(TestJoinTreeCostEstYannakakisProvider.class.getName());

    private static final String naturalJoinTable = "TestJoinTreeCostEstYannakakisProvider";
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
            Configurator.setAllLevels(TestJoinTreeCostEstYannakakisProvider.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(JoinTreeCostEstYannakakisProvider.class.getName(), Level.DEBUG);
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
        JoinTreeCostEstYannakakisProvider.CostEstimateQueryBuilder costEstimateQueryBuilder =
                new JoinTreeCostEstYannakakisProvider.CostEstimateQueryBuilder(postgresJdbcClient.getIdentifierQuote(), false);
        try (Connection connection = postgresJdbcClient.getConnection()) {
            for (MultiwayJoinNode node : joinTree.getTraversalList()) {
                if (node.equals(infoTypeNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, movieInfoIdxNode, List.of(), joinTree)) {
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\" WHERE (info_type_id) IN ( SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")))",
                                preparedStatement.toString());
                        assertTrue(preparedStatement.execute());
                    }
                }
                else if (node.equals(movieInfoIdxNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, movieCompaniesNode, List.of(), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")) UNION ALL SELECT movie_id,info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")) AND (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\" WHERE (info_type_id) IN ( SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\"))))",
                                preparedStatement.toString());
                    }
                }
                else if (node.equals(movieCompaniesNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, companyTypeNode, List.of(), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,company_id,company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\") UNION ALL SELECT movie_id,company_id,company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\") AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\") AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")) AND (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\" WHERE (info_type_id) IN ( SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")))))",
                                preparedStatement.toString());
                    }
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, titleNode, List.of(companyTypeNode), joinTree)) {
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON)  SELECT movie_id,company_id,company_type_id FROM (SELECT movie_id,company_id,company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")) AS alias0 WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")",
                                preparedStatement.toString());
                        assertTrue(preparedStatement.execute());
                    }
                }
                else if (node.equals(companyTypeNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, null, List.of(), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON) SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\") AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\") AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")) AND (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\" WHERE (info_type_id) IN ( SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\"))))))",
                                preparedStatement.toString());
                    }
                }
                else if (node.equals(titleNode)) {
                    try (PreparedStatement preparedStatement = costEstimateQueryBuilder
                            .buildCostSql(postgresJdbcClient, connection, node, null, List.of(), joinTree)) {
                        assertTrue(preparedStatement.execute());
                        assertEquals("EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON) SELECT movie_id,kind_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\" WHERE (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\") AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\") AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\")) AND (info_type_id) IN (SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstinfo_type\" WHERE (info_type_id) IN ( SELECT info_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_info_idx\" WHERE (movie_id) IN ( SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstmovie_companies\" WHERE (company_type_id) IN (SELECT company_type_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEstcompany_type\")AND (movie_id) IN (SELECT movie_id FROM \"postgres\".\"multiwaycomplex\".\"JTCEsttitle\"))))))",
                                preparedStatement.toString());
                    }
                }
            }
        }
    }

    public static class TestCostModelCases
            implements TestCases
    {
        TestJoinTreeCostEstProvider.TestCostModelCases cases;
        String schemaName;
        JdbcClient jdbcClient;
        TestingPhysicalPlanBase base;

        public TestCostModelCases(TestingPhysicalPlanBase base)
        {
            cases = new TestJoinTreeCostEstProvider.TestCostModelCases(base);
            schemaName = base.getDatabase().getSchemaName();
            jdbcClient = base.getDatabase().getJdbcClient();
            this.base = base;
        }

        public Triple<Plan, List<Operator>, Integer> case1()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case1();
            return Triple.of(test.getLeft(), test.getMiddle(), 7);
        }

        public Triple<Plan, List<Operator>, Integer> case2()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case2();
            return Triple.of(test.getLeft(), test.getMiddle(), 7);
        }

        public Triple<Plan, List<Operator>, Integer> case3()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case3();
            return Triple.of(test.getLeft(), test.getMiddle(), 4);
        }

        public Triple<Plan, List<Operator>, Integer> case4()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case4();
            return Triple.of(test.getLeft(), test.getMiddle(), 0);
        }
    }

    private Object[][] testCostModelDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestCostModelCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testCostModelDataProvider")
    public void testEstimateCostOfYannakakis(Triple<Plan, List<Operator>, Integer> triple)
    {
        int expectedValue = triple.getRight();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .build();
        JoinTreeCostEstYannakakisProvider costEstProvider = new JoinTreeCostEstYannakakisProvider(config);
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
        TestJoinTreeCostEstProvider.TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCases cases;

        public TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCases(TestingPhysicalPlanBase base)
        {
            cases = new TestJoinTreeCostEstProvider.TestCostModelEstimateSizeOfIntermediateResultsThatArePartOfFinalJoinResultCases(base);
        }

        public Triple<Plan, List<Operator>, Integer> case1()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case1();
            return Triple.of(test.getLeft(), test.getMiddle(), 12);
        }

        public Triple<Plan, List<Operator>, Integer> case2()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case2();
            return Triple.of(test.getLeft(), test.getMiddle(), 11);
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
        JoinTreeCostEstYannakakisProvider costEstProvider = new JoinTreeCostEstYannakakisProvider(config);
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
        TestJoinTreeCostEstProvider.TestCostModelIncludeInnerRelationCleanStateSizeCases cases;

        public TestCostModelIncludeInnerRelationCleanStateSizeCases(TestingPhysicalPlanBase base)
        {
            cases = new TestJoinTreeCostEstProvider.TestCostModelIncludeInnerRelationCleanStateSizeCases(base);
        }

        public Triple<Plan, List<Operator>, Integer> case1()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case1();
            return Triple.of(test.getLeft(), test.getMiddle(), 16);
        }

        public Triple<Plan, List<Operator>, Integer> case2()
        {
            Triple<Plan, List<Operator>, Integer> test = cases.case2();
            return Triple.of(test.getLeft(), test.getMiddle(), 14);
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
        JoinTreeCostEstYannakakisProvider costEstProvider = new JoinTreeCostEstYannakakisProvider(config);
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

    public static class TestDifferentSemijoinOrderingCases
            implements TestCases
    {
        TestingPhysicalPlanBase base;
        String schemaName;
        JdbcClient jdbcClient;

        public static class DifferentSemijoinOrderingReturnCollection
        {
            private final Plan plan;
            private final List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedBottomUpPassOrdering;
            private final JoinTreeCostProviderConfiguration config;
            private final Double expectedCost;

            public DifferentSemijoinOrderingReturnCollection(Plan plan,
                                                             List<Pair<MultiwayJoinNode, MultiwayJoinNode>> expectedBottomUpPassOrdering,
                                                             JoinTreeCostProviderConfiguration config,
                                                             Double expectedCost)
            {
                this.plan = plan;
                this.expectedBottomUpPassOrdering = expectedBottomUpPassOrdering;
                this.config = config;
                this.expectedCost = expectedCost;
            }
        }

        public TestDifferentSemijoinOrderingCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
            this.schemaName = base.getDatabase().getSchemaName();
            this.jdbcClient = base.getDatabase().getJdbcClient();
        }

        public DifferentSemijoinOrderingReturnCollection case1()
        {
            String relationT = "casefour_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            List<String> attributesT = new ArrayList<>(List.of("x"));
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(VARCHAR, "green")),
                        Collections.singletonList(new StringValue(VARCHAR, "green")),
                        Collections.singletonList(new StringValue(VARCHAR, "red"))));
                jdbcClient.ingestRelation(schemaName, relationT, attributesT, new ArrayList<>(List.of(VARCHAR)), relationValT);
            }
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, attributesT, new MultiwayJoinDomain());

            String relationS = "casefour_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            List<String> attributeS = new ArrayList<>(Arrays.asList("x", "y", "z"));
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        List.of(new StringValue(VARCHAR, "red"), new StringValue(VARCHAR, "1"), new StringValue(VARCHAR, "2")),
                        Arrays.asList(new StringValue(VARCHAR, "red"), new StringValue(VARCHAR, "3"), new StringValue(VARCHAR, "2"))));
                jdbcClient.ingestRelation(schemaName, relationS, attributeS, new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)), relationValS);
            }
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributeS, new MultiwayJoinDomain());

            String relationB = "casefour_B";
            SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
            List<String> attributeB = new ArrayList<>(List.of("z"));
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(Collections.singletonList(new StringValue(VARCHAR, "2"))));
                jdbcClient.ingestRelation(schemaName, relationB, attributeB, new ArrayList<>(List.of(VARCHAR)), relationValB);
            }
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributeB, new MultiwayJoinDomain());

            String relationR = "casefour_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            List<String> attributeR = new ArrayList<>(Arrays.asList("y", "z"));
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(Arrays.asList(new StringValue(VARCHAR, "3"), new StringValue(VARCHAR, "2"))));
                jdbcClient.ingestRelation(schemaName, relationR, attributeR, new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)), relationValR);
            }
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributeR, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeB), asEdge(nodeS, nodeR))), nodeT);

            SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(
                    List.of(org.apache.commons.lang3.tuple.Pair.of(nodeS, nodeR), org.apache.commons.lang3.tuple.Pair.of(nodeS, nodeB), org.apache.commons.lang3.tuple.Pair.of(nodeT, nodeS)),
                    orderedGraph);

            Pair<Plan, List<Operator>> pair = base.createPhysicalPlanForYannakakis(semiJoinOrdering);
            return new DifferentSemijoinOrderingReturnCollection(pair.getLeft(),
                    List.of(Pair.of(nodeS, nodeR),
                            Pair.of(nodeS, nodeB),
                            Pair.of(nodeT, nodeS)),
                    JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                            .useTrueCard(true)
                            .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(false)
                            .includeInnerRelationSize(false)
                            .enableGreedySemijoinOrdering(true)
                            .build(),
                    6.0);
        }

        public DifferentSemijoinOrderingReturnCollection case2()
        {
            String relationT = "casefour_T";
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
            List<String> attributesT = new ArrayList<>(List.of("x"));
            if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
                List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(VARCHAR, "green")),
                        Collections.singletonList(new StringValue(VARCHAR, "green")),
                        Collections.singletonList(new StringValue(VARCHAR, "red"))));
                jdbcClient.ingestRelation(schemaName, relationT, attributesT, new ArrayList<>(List.of(VARCHAR)), relationValT);
            }
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, attributesT, new MultiwayJoinDomain());

            String relationS = "casefour_S";
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            List<String> attributeS = new ArrayList<>(Arrays.asList("x", "y", "z"));
            if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
                List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                        List.of(new StringValue(VARCHAR, "red"), new StringValue(VARCHAR, "1"), new StringValue(VARCHAR, "2")),
                        Arrays.asList(new StringValue(VARCHAR, "red"), new StringValue(VARCHAR, "3"), new StringValue(VARCHAR, "2"))));
                jdbcClient.ingestRelation(schemaName, relationS, attributeS, new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)), relationValS);
            }
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributeS, new MultiwayJoinDomain());

            String relationB = "casefour_B";
            SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
            List<String> attributeB = new ArrayList<>(List.of("z"));
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(Collections.singletonList(new StringValue(VARCHAR, "2"))));
                jdbcClient.ingestRelation(schemaName, relationB, attributeB, new ArrayList<>(List.of(VARCHAR)), relationValB);
            }
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributeB, new MultiwayJoinDomain());

            String relationR = "casefour_R";
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            List<String> attributeR = new ArrayList<>(Arrays.asList("y", "z"));
            if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
                List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(Arrays.asList(new StringValue(VARCHAR, "3"), new StringValue(VARCHAR, "2"))));
                jdbcClient.ingestRelation(schemaName, relationR, attributeR, new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)), relationValR);
            }
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributeR, new MultiwayJoinDomain());

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeB), asEdge(nodeS, nodeR))), nodeT);

            SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(
                    List.of(org.apache.commons.lang3.tuple.Pair.of(nodeS, nodeR), org.apache.commons.lang3.tuple.Pair.of(nodeS, nodeB), org.apache.commons.lang3.tuple.Pair.of(nodeT, nodeS)),
                    orderedGraph);

            Pair<Plan, List<Operator>> pair = base.createPhysicalPlanForYannakakis(semiJoinOrdering);
            return new DifferentSemijoinOrderingReturnCollection(pair.getLeft(),
                    List.of(Pair.of(nodeS, nodeB),
                            Pair.of(nodeS, nodeR),
                            Pair.of(nodeT, nodeS)),
                    JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                            .useTrueCard(true)
                            .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(false)
                            .includeInnerRelationSize(false)
                            .build(),
                    7.0);
        }
    }

    private Object[][] testDifferentSemijoinOrderingDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestDifferentSemijoinOrderingCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testDifferentSemijoinOrderingDataProvider")
    public void testDifferentSemijoinOrdering(TestDifferentSemijoinOrderingCases.DifferentSemijoinOrderingReturnCollection collection)
    {
        JoinTreeCostEstYannakakisProvider costEstProvider = new JoinTreeCostEstYannakakisProvider(collection.config);
        JoinTreeCostReturn costReturn = costEstProvider.getCost(new JoinOrdering(getSchemaTableNames(collection.plan.getRoot())),
                collection.plan.getRoot().getOperator().getPlanBuildContext().getOrderedGraph(),
                collection.plan.getRoot().getOperator().getPlanBuildContext());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug(costReturn.getCost());
        }
        assertEquals(collection.expectedBottomUpPassOrdering, costReturn.getSemiJoinOrdering().getBottomUpPass());
        assertEquals(collection.expectedCost, costReturn.getCost());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}