package org.zhu45.treetracker.benchmark;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformationContext;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformationFactory;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TTJAggregateStatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostReturn;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.execution.ExecutionBase.cleanUp;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.StatisticsInformationToJson.generateStatisticsInformationJson;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

/**
 * Unlike TestJoinTreeCostEstProvider, this class
 * tests using the real benchmark data.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJoinTreeCostEstProviderWithBenchmark
{
    private static final Logger traceLogger;

    private static final String naturalJoinTable = "TestJoinTreeCostEstProviderWithBenchmark";
    private TestingPhysicalPlanBase base;
    private IdAllocator idAllocator;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TestJoinTreeCostEstProviderWithBenchmark.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(JoinTreeCostEstProvider.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TestJoinTreeCostEstProviderWithBenchmark.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.ERROR);
            Configurator.setAllLevels(TupleBasedHighPerfTableScanOperator.class.getName(), Level.ERROR);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                        Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class))));
        idAllocator = new IdAllocator();
    }

    public static class TestCostModelCases
            implements TestCases
    {
        TestingPhysicalPlanBase base;

        public TestCostModelCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
        }

        public Pair<Plan, List<Operator>> case1()
        {
            MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q1a, null);
            MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q1a, null);
            List<MultiwayJoinNode> traversalList = List.of(movieCompaniesNode, movieInfoIdxNode);
            MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(movieCompaniesNode, Arrays.asList(
                    asDirectedEdge(movieCompaniesNode, movieInfoIdxNode)), traversalList);
            return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                    Optional.empty(),
                    Optional.empty());
        }

        public Pair<Plan, List<Operator>> case2()
        {
            MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q10c);
            MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q10c);
            List<MultiwayJoinNode> traversalList = List.of(castInfoNode, charNameNode);
            MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(castInfoNode, Arrays.asList(
                    asDirectedEdge(castInfoNode, charNameNode)), traversalList);
            return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                    Optional.empty(),
                    Optional.empty());
        }

        public Pair<Plan, List<Operator>> case3()
        {
            MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q10c, null);
            MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q10c);
            MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q10c, null);
            MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q10c, null);
            List<MultiwayJoinNode> traversalList = List.of(companyNameNode, movieCompaniesNode, companyTypeNode, titleNode);
            MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(companyNameNode, Arrays.asList(
                    asDirectedEdge(companyNameNode, movieCompaniesNode),
                    asDirectedEdge(movieCompaniesNode, companyTypeNode),
                    asDirectedEdge(movieCompaniesNode, titleNode)), traversalList);
            return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph,
                    Optional.empty(),
                    Optional.empty());
        }
    }

    private Object[][] testCostModelDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestCostModelCases.class)));
    }

    @DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
    @ParameterizedTest
    @MethodSource("testCostModelDataProvider")
    public void test(Pair<Plan, List<Operator>> pair)
    {
        Pair<Long, Double> retPair = getNumBerOfDanglingTuplesAndCost(pair);
        assertEquals(retPair.getLeft(), retPair.getRight().longValue());
    }

    private Pair<Long, Double> getNumBerOfDanglingTuplesAndCost(Pair<Plan, List<Operator>> pair)
    {
        ExecutionNormal executionNormal = new ExecutionNormal(pair.getLeft().getRoot());
        executionNormal.evalForBenchmark();
        Operator rootOperator = pair.getLeft().getRoot().getOperator();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.TTJHP)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        TTJAggregateStatisticsInformation aggregateStatisticsInformation = (TTJAggregateStatisticsInformation) factory.get();
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            generateStatisticsInformationJson(rootOperator, aggregateStatisticsInformation, JoinOperator.TTJHP,
                    idAllocator.getNextId(), ".");
        }
        cleanUp(pair.getRight());
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(true)
                .build();
        JoinTreeCostEstProvider costEstProvider = new JoinTreeCostEstProvider(config);
        JoinTreeCostReturn costReturn = costEstProvider.getCost(new JoinOrdering(getSchemaTableNames(pair.getLeft().getRoot())),
                pair.getLeft().getRoot().getOperator().getPlanBuildContext().getOrderedGraph(),
                pair.getLeft().getRoot().getOperator().getPlanBuildContext());
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("sqls: " + costReturn.getSqls());
        }
        return Pair.of(aggregateStatisticsInformation.getNumberOfDanglingTuples(), costReturn.getCost());
    }

    /**
     * This test case illustrates the limitation of current cost model implementation: the cost model
     * implementation doesn't consider join ordering in its SQL generation. Details see cost-model6 remark
     * section. The test is simplified from the situation found by Q10c DP. The cell of the original DP that exposed
     * this issue can be constructed as following:
     * <pre>
     * {@code
     *   MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q10c, null);
     *   MultiwayJoinNode companyNameNode = getCompanyNameInt(JOBQueries.Q10c, null);
     *   MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q10c);
     *   MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q10c, null);
     *   MultiwayJoinNode charNameNode = getCharNameInt(JOBQueries.Q10c);
     *
     *   List<MultiwayJoinNode> traversalList = List.of(castInfoNode, charNameNode, movieCompaniesNode, titleNode, companyNameNode);
     *   MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(castInfoNode, Arrays.asList(
     *                     asDirectedEdge(castInfoNode, charNameNode),
     *                     asDirectedEdge(castInfoNode, movieCompaniesNode),
     *                     asDirectedEdge(movieCompaniesNode, companyNameNode),
     *                     asDirectedEdge(castInfoNode, titleNode)), traversalList);
     *   return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.empty(), Optional.empty());
     * }
     * </pre>
     */
    @Test
    public void costModelImplementationLimitation()
    {
        String schemaName = base.getDatabase().getSchemaName();
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String relationPrefix = this.getClass().getSimpleName() + "_costModelLim";

        String relationCastInfo = relationPrefix + "_ci";
        SchemaTableName schemaTableNameCastInfo = new SchemaTableName(schemaName, relationCastInfo);
        List<String> attributeCastInfo = List.of("movie_id");
        if (jdbcClient.getTableHandle(schemaTableNameCastInfo) == null) {
            List<List<RelationalValue>> relationValCastInfo = new ArrayList<>(Arrays.asList(
                    Arrays.asList(IntegerValue.of(1926078))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationCastInfo,
                    attributeCastInfo,
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relationValCastInfo);
        }
        MultiwayJoinNode nodeCI = new MultiwayJoinNode(schemaTableNameCastInfo, attributeCastInfo, new MultiwayJoinDomain());

        String relationMovieCompanies = relationPrefix + "_mc";
        SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
        List<String> attributesMovieCompanies = List.of("movie_id", "company_id");
        if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
            List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                    Arrays.asList(IntegerValue.of(1926078), IntegerValue.of(7549))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationMovieCompanies,
                    attributesMovieCompanies,
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relationValMovieCompanies);
        }
        MultiwayJoinNode nodeMC = new MultiwayJoinNode(schemaTableNameMovieCompanies, attributesMovieCompanies, new MultiwayJoinDomain());

        String relationTitle = relationPrefix + "_title";
        SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, relationTitle);
        List<String> attributesTitle = List.of("movie_id");
        if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
            List<List<RelationalValue>> relationValTitle = new ArrayList<>(Arrays.asList(
                    Arrays.asList(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationTitle,
                    attributesTitle,
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relationValTitle);
        }
        MultiwayJoinNode nodeTitle = new MultiwayJoinNode(schemaTableNameTitle, attributesTitle, new MultiwayJoinDomain());

        String relationCompanyName = relationPrefix + "_cn";
        SchemaTableName schemaTableNameCompanyName = new SchemaTableName(schemaName, relationCompanyName);
        List<String> attributesCompanyName = List.of("company_id");
        if (jdbcClient.getTableHandle(schemaTableNameCompanyName) == null) {
            List<List<RelationalValue>> relationValCompanyName = new ArrayList<>(Arrays.asList(
                    Arrays.asList(IntegerValue.of(1))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationCompanyName,
                    attributesCompanyName,
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relationValCompanyName);
        }
        MultiwayJoinNode nodeCN = new MultiwayJoinNode(schemaTableNameCompanyName, attributesCompanyName, new MultiwayJoinDomain());

        List<MultiwayJoinNode> traversalList = List.of(nodeCI, nodeMC, nodeTitle, nodeCN);

        MultiwayJoinOrderedGraph joinTree = new MultiwayJoinOrderedGraph(nodeCI, Arrays.asList(
                asDirectedEdge(nodeCI, nodeMC),
                asDirectedEdge(nodeMC, nodeCN),
                asDirectedEdge(nodeCI, nodeTitle)), traversalList);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(joinTree,
                Optional.empty(),
                Optional.empty());
        Pair<Long, Double> retPair = getNumBerOfDanglingTuplesAndCost(pair);
        assertEquals(retPair.getLeft(), 2);
        assertEquals(retPair.getRight(), 3);
        assertThrows(AssertionFailedError.class, () -> {
            assertEquals(retPair.getLeft(), retPair.getRight().longValue());
        });
    }

    private static class IdAllocator
    {
        private int nextId;

        public String getNextId()
        {
            return "alias" + nextId++;
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
