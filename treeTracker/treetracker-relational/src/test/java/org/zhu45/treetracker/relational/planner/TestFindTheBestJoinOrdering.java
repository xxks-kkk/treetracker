package org.zhu45.treetracker.relational.planner;

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
import org.zhu45.treetracker.relational.operator.DatabaseSuppler;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.operator.testCases.TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases;
import org.zhu45.treetracker.relational.planner.plan.PlanStatistics;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.statistics.CostModelStatisticsData;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestFindTheBestJoinOrdering
{
    private static final String naturalJoinTable = "TestFindTheBestJoinOrdering";
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
            Configurator.setAllLevels(FindTheBestJoinOrdering.class.getName(), Level.DEBUG);
        }

        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(new FindTheBestJoinOrdering()),
                ExecutionNormal.class,
                database.getSeed(),
                Optional.of(createMap(Optional.empty(), Optional.of(TupleBasedHashJoinOperator.class))));
    }

    private Stream<Pair<Plan, List<Operator>>> testFindTheBestJoinOrderingDataProvider()
    {
        TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases cases = new TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(base);
        return Stream.of(cases.testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour());
    }

    @ParameterizedTest
    @MethodSource("testFindTheBestJoinOrderingDataProvider")
    public void testFindTheBestJoinOrdering(Pair<Plan, List<Operator>> pair)
    {
        Plan plan = pair.getKey();
        PlanStatistics planStatistics = plan.getPlanStatistics();
        HashMap<JoinOrdering, Float> searchedPlan = planStatistics.getSearchedPlan();
        assertEquals(14, searchedPlan.size());
        searchedPlan.keySet().forEach(key -> assertTrue(searchedPlan.get(key) != 0));
        JoinOrdering optimalJoinOrdering = planStatistics.getOptimalJoinOrdering();
        assertTrue(base.getCaseVerifier().visitPlan(plan.getRoot(), new LinkedList<>(optimalJoinOrdering.getSchemaTableNameList())));
        save(planStatistics);
        base.testPhysicalPlanExecution(pair);
    }

    private void save(PlanStatistics planStatistics)
    {
        String queryName = "dummy";
        Path path = Paths.get("src", "test", "resources", "unitTestDummyResult");
        File file = path.toFile();
        file.mkdirs();
        Path savedPath = planStatistics.save(queryName, JoinOperator.HASH_JOIN, path.toString());
        try {
            try (Stream<String> stream = Files.lines(savedPath)) {
                assertTrue(stream.count() > 1);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
