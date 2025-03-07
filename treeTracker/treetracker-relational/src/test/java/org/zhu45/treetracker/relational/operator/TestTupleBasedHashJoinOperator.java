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
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestCases;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.twoDlistTo2DArray;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildAllTestCases;
import static org.zhu45.treetracker.relational.operator.testCases.InstantiateTestCases.buildSpecificTestCases;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.testing.postgresplan.PostgresPlanGenerator.generatePostgresPlanJson;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedHashJoinOperator
{
    private static final String naturalJoinTable = "TestTupleBasedHashJoinOperator";
    private TestingPhysicalPlanBase base;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(DatabaseSuppler.class.getName(), Level.INFO);
            Configurator.setAllLevels(TestingMultiwayJoinDatabaseComplex.class.getName(), Level.DEBUG);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.empty(), Optional.of(TupleBasedHashJoinOperator.class))));
    }

    private Object[][] testTupleBasedHashJoinOperatorTestCasesDataProvider()
    {
        return twoDlistTo2DArray(buildAllTestCases(base));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedHashJoinOperatorTestCasesDataProvider")
    public void testTupleBasedHashJoinOperator(Pair<Plan, List<Operator>> pair)
    {
        base.testPhysicalPlanExecution(pair);
    }

    public static class TestBushyPlanTestCases
            implements TestCases
    {
        String schemaName;
        TestingPhysicalPlanBase base;
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases;

        public TestBushyPlanTestCases(TestingPhysicalPlanBase base)
        {
            this.base = base;
            this.cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
            this.schemaName = base.getDatabase().getSchemaName();
        }

        /**
         * Test based on testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne
         * JOIN,Outer
         * |JOIN,Inner
         * ||TAB,caseone_T,Outer
         * ||TAB,caseone_S,Inner
         * |JOIN,Outer
         * ||TAB,caseone_B,Inner
         * ||TAB,caseone_R,Outer
         */
        public Pair<Plan, List<Operator>> test1()
        {
            cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
            String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                    "|JOIN,Inner\n" +
                    "||TAB,caseone_T,Outer\n" +
                    "||TAB,caseone_S,Inner\n" +
                    "|JOIN,Outer\n" +
                    "||TAB,caseone_B,Inner\n" +
                    "||TAB,caseone_R,Outer");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, schemaName);
        }

        /**
         * This is the same as test1() but we test the other way of creating a plan from Postgres plan by providing schemaTableNameList.
         */
        public Pair<Plan, List<Operator>> test2()
        {
            Pair<Plan, List<Operator>> pair = cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
            String postgresPlan = generatePostgresPlanJson("JOIN,Outer,1,1\n" +
                    "|JOIN,Inner,10,6\n" +
                    "||TAB,caseone_R,Outer,10,10\n" +
                    "||HASH,Inner,10,20\n" +
                    "|||JOIN,Outer,10,20\n" +
                    "||||TAB,caseone_T,Inner,5,100\n" +
                    "||||TAB,caseone_S,Outer,100,14\n" +
                    "|TAB,caseone_B,Outer,10,100");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, pair.getKey().getRoot().getOperator().getPlanBuildContext().getSchemaTableNameList());
        }

        /**
         * Test the case where the child of a single-child node is also single-child. For example, SORT has a single-child GARTHER,
         * which also has a single-child JOIN. Further, the test tests the second JOIN should be properly set to Inner once it is loaded
         * into our system. Note that, the parent of JOIN is GATHER, which has Outer. Inner comes from SORT.
         */
        public Pair<Plan, List<Operator>> test3()
        {
            cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
            String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                    "|TAB,caseone_R,Outer\n" +
                    "|SORT,Inner\n" +
                    "||GATHER,Outer\n" +
                    "|||JOIN,Outer\n" +
                    "||||TAB,caseone_T,Outer\n" +
                    "||||TAB,caseone_S,Inner");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, schemaName);
        }

        /**
         * Test correct support for MATERIALIZE NodeType from Postgres plan.
         */
        public Pair<Plan, List<Operator>> test4()
        {
            cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
            String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                    "|TAB,caseone_R,Outer\n" +
                    "|MATERIALIZE,Inner\n" +
                    "||GATHER,Outer\n" +
                    "|||JOIN,Outer\n" +
                    "||||TAB,caseone_T,Outer\n" +
                    "||||TAB,caseone_S,Inner");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, schemaName);
        }

        /**
         * Test support for Gather Merge Node from Postgres plan.
         */
        public Pair<Plan, List<Operator>> test5()
        {
            cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
            String postgresPlan = generatePostgresPlanJson("JOIN,Outer\n" +
                    "|TAB,caseone_R,Outer\n" +
                    "|MATERIALIZE,Inner\n" +
                    "||GATHER_MERGE,Outer\n" +
                    "|||JOIN,Outer\n" +
                    "||||TAB,caseone_T,Outer\n" +
                    "||||TAB,caseone_S,Inner");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, schemaName);
        }

        /**
         * Test support for Aggregate Node from Postgres plan. The plan is inspired from JOB 1a.
         */
        public Pair<Plan, List<Operator>> test6()
        {
            cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
            String postgresPlan = generatePostgresPlanJson("AGG,Outer\n" +
                    "|GATHER,Outer\n" +
                    "||AGG,Outer\n" +
                    "|||JOIN,Outer\n" +
                    "||||TAB,caseone_T,Outer\n" +
                    "||||TAB,caseone_S,Inner");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, schemaName);
        }

        /**
         * This test checks the rootOperatorPlanNode is properly set. The test case is inspired from JOB 1a.
         */
        public Pair<Plan, List<Operator>> test7()
        {
            String relationTitle = "caseseven_title";
            SchemaTableName schemaTableNameTitle = new SchemaTableName(schemaName, relationTitle);
            JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
            if (jdbcClient.getTableHandle(schemaTableNameTitle) == null) {
                List<List<RelationalValue>> relationValTitle = new ArrayList<>(Arrays.asList(
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(1)),
                        Arrays.asList(IntegerValue.of(1), IntegerValue.of(2))));
                jdbcClient.ingestRelation(schemaName, relationTitle,
                        new ArrayList<>(List.of("movie_id", "kind_id")),
                        new ArrayList<>(List.of(INTEGER, INTEGER)), relationValTitle);
            }

            String relationMovieInfoIdx = "caseseven_movie_info_idx";
            SchemaTableName schemaTableNameMovieInfoIdx = new SchemaTableName(schemaName, relationMovieInfoIdx);
            if (jdbcClient.getTableHandle(schemaTableNameMovieInfoIdx) == null) {
                List<List<RelationalValue>> relationValMovieInfoIdx = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(1)),
                        List.of(IntegerValue.of(0), IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationMovieInfoIdx,
                        new ArrayList<>(List.of("movie_id", "info_type_id")),
                        new ArrayList<>(List.of(INTEGER, INTEGER)), relationValMovieInfoIdx);
            }

            String relationMovieCompanies = "caseseven_movie_companies";
            SchemaTableName schemaTableNameMovieCompanies = new SchemaTableName(schemaName, relationMovieCompanies);
            if (jdbcClient.getTableHandle(schemaTableNameMovieCompanies) == null) {
                List<List<RelationalValue>> relationValMovieCompanies = new ArrayList<>(Arrays.asList(
                        List.of(IntegerValue.of(1), IntegerValue.of(1), IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationMovieCompanies, new ArrayList<>(List.of("movie_id", "company_id", "company_type_id")),
                        new ArrayList<>(List.of(INTEGER, INTEGER, INTEGER)), relationValMovieCompanies);
            }

            String relationCompanyType = "caseseven_company_type";
            SchemaTableName schemaTableNameCompanyType = new SchemaTableName(schemaName, relationCompanyType);
            if (jdbcClient.getTableHandle(schemaTableNameCompanyType) == null) {
                List<List<RelationalValue>> relationValCompanyType = new ArrayList<>(Arrays.asList(
                        Collections.singletonList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationCompanyType, new ArrayList<>(List.of("company_type_id")),
                        new ArrayList<>(List.of(INTEGER)), relationValCompanyType);
            }

            String relationInfoType = "caseseven_info_type";
            SchemaTableName schemaTableNameInfoType = new SchemaTableName(schemaName, relationInfoType);
            if (jdbcClient.getTableHandle(schemaTableNameInfoType) == null) {
                List<List<RelationalValue>> relationValInfoType = new ArrayList<>(Arrays.asList(
                        Collections.singletonList(IntegerValue.of(1))));
                jdbcClient.ingestRelation(schemaName, relationInfoType, new ArrayList<>(List.of("info_type_id")),
                        new ArrayList<>(List.of(INTEGER)), relationValInfoType);
            }

            String postgresPlan = generatePostgresPlanJson("AGG,Outer\n" +
                    "|GATHER,Outer\n" +
                    "||AGG,Outer\n" +
                    "|||JOIN,Outer\n" +
                    "||||TAB,caseseven_title,Outer\n" +
                    "||||HASH,Inner,\n" +
                    "|||||JOIN,Outer\n" +
                    "||||||TAB,caseseven_info_type,Inner\n" +
                    "||||||JOIN,Outer\n" +
                    "|||||||TAB,caseseven_movie_info_idx,Outer\n" +
                    "|||||||HASH,Inner\n" +
                    "||||||||JOIN,Outer\n" +
                    "|||||||||TAB,caseseven_movie_companies,Outer\n" +
                    "|||||||||HASH,Inner\n" +
                    "||||||||||TAB,caseseven_company_type,Outer");
            return base.createPhysicalPlanFromPostgresPlan(postgresPlan, schemaName);
        }
    }

    private Object[][] testBushyPlanDataProvider()
    {
        return twoDlistTo2DArray(buildSpecificTestCases(base, List.of(TestBushyPlanTestCases.class)));
    }

    @ParameterizedTest
    @MethodSource("testBushyPlanDataProvider")
    public void testTupleBasedHashJoinOperatorBushyPlan(Pair<Plan, List<Operator>> pair)
    {
        base.testPhysicalPlanExecution(pair);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
