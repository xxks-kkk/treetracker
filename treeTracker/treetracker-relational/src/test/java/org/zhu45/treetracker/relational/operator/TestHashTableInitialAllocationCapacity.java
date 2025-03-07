package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBasedTreeTrackerTwoOperatorCases;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.testing.postgresplan.PostgresPlanGenerator.generatePostgresPlanJson;

/**
 * This tests the performance optimization of preallocating hash table size
 * works as expected for algorithms of hash join family.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHashTableInitialAllocationCapacity
{
    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestHashTableInitialAllocationCapacity";
    private StatisticsInformationPrinter printer;
    private TestTupleBasedTreeTrackerTwoOperatorCases casesForRightDeepPlan;
    private TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases casesForLeftDeepPlan;

    @BeforeAll
    public void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBaseTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                Collections.emptyList(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class))));

        casesForRightDeepPlan = new TestTupleBasedTreeTrackerTwoOperatorCases(base);
        casesForLeftDeepPlan = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        printer = new StatisticsInformationPrinter();
    }

    @Test
    public void testForRightDeepPlan()
    {
        Pair<Plan, List<Operator>> pair = casesForRightDeepPlan.testTupleBasedTreeTrackerTwoOperatorCaseOne();
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 13"));
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 2"));
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 7"));
    }

    @Test
    public void testForLeftDeepPlan()
    {
        Pair<Plan, List<Operator>> pair = casesForLeftDeepPlan.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertEquals(2, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 5"));
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 3"));
    }

    /**
     * We test the following bushy plan
     * <pre>
     * JOIN,Outer,1,1
     * |JOIN,Inner,10,6
     * ||TAB,caseone_R,Outer,10,10
     * ||HASH,Inner,10,20
     * |||JOIN,Outer,10,20
     * ||||TAB,caseone_T,Inner,5,100
     * ||||TAB,caseone_S,Outer,100,14
     * |TAB,caseone_B,Outer,10,100
     * </pre>
     * We expect that:
     * <ul>
     * <li>We always use ground truth for Inner Table scan regardless what Postgres plan says in actual rows (Technically, actual rows in a real Postgres
     * plan is also ground truth; in our implementation, we take the most safe option by reading from Catalog)
     * <li>We use actual rows whenever possible and discard plan rows if actual rows present
     * </ul>
     * <p>
     * We use {@code (int) ((float) operatorAssociatedRelationSize / 0.75F + 1.0F)} formula. Thus, we expect the following hashTableAllocationInitialCapacity:
     * <ul>
     * <li>{@code JOIN,Inner,10,6} --> 6 / 0.75 + 1 = 9
     * <li>{@code HASH,Inner,10,20} --> 20 / 0.75 + 1 = 27
     * <li>{@code TAB,caseone_T,Inner,5,100} --> 3 / 0.75 + 1 = 5. Note we purposefully lie in our plan by setting actual rows to be 100 but in fact, the ground truth is 3.
     * </ul>
     */
    @Test
    public void testForPostgresPlan()
    {
        casesForLeftDeepPlan.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer,1,1\n" +
                "|JOIN,Inner,10,6\n" +
                "||TAB,caseone_R,Outer,10,10\n" +
                "||HASH,Inner,10,20\n" +
                "|||JOIN,Outer,10,20\n" +
                "||||TAB,caseone_T,Inner,5,100\n" +
                "||||TAB,caseone_S,Outer,100,14\n" +
                "|TAB,caseone_B,Outer,10,100");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 9"));
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 27"));
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 5"));
    }

    /**
     * This test aims to test implementation of setOperatorAssociatedRelationSizeForJoin() in the case of table scan operator is
     * connected under a HASH node.
     */
    @Test
    public void testForPostgresPlan2()
    {
        casesForLeftDeepPlan.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer,1,1\n" +
                "|HASH,Inner,10,6\n" +
                "||TAB,caseone_R,Outer,10,10\n" +
                "|TAB,caseone_B,Outer,10,100");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 5"));
    }

    /**
     * This test case tests the situation where JOIN node is below HASH node. In such case, JOIN node has operator but
     * it is not table scan operator.
     */
    @Test
    public void testForPostgresPlan3()
    {
        casesForLeftDeepPlan.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne();
        String postgresPlan = generatePostgresPlanJson("JOIN,Outer,1,1\n" +
                "|HASH,Inner,10,20\n" +
                "||JOIN,Outer,20,20\n" +
                "|||TAB,caseone_R,Outer,10,10\n" +
                "|||TAB,caseone_S,Inner,10,100\n" +
                "|TAB,caseone_B,Outer,10,100");
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromPostgresPlan(postgresPlan, base.getDatabase().getSchemaName());
        base.testPhysicalPlanExecution(pair);
        Operator rootOperator = pair.getKey().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        assertEquals(1, StringUtils.countMatches(statistics, "hashTableAllocationInitialCapacity: 27"));
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
