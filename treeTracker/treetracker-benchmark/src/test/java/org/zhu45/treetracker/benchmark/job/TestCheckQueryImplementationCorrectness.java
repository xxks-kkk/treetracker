package org.zhu45.treetracker.benchmark.job;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.ssb.SSBQueries;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesFindOptJoinTree;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesShallowHJOrdering;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavIntRowNoGoodListMap;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.gatherOuterTables;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.getLeftMostNode;

@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCheckQueryImplementationCorrectness
{
    private Stream<Arguments> testCheckQueryImplementationCorrectnessDataProvider()
    {
        return Stream.of(
                Arguments.of(JOBQueriesFindOptJoinTree.Query1aFindOptJoinTree, 142),
                Arguments.of(JOBQueriesFindOptJoinTree.Query1bFindOptJoinTree, 3),
                Arguments.of(JOBQueriesFindOptJoinTree.Query1cFindOptJoinTree, 3),
                Arguments.of(JOBQueriesFindOptJoinTree.Query1dFindOptJoinTree, 4),
                Arguments.of(JOBQueriesFindOptJoinTree.Query2aFindOptJoinTree, 7834),
                Arguments.of(JOBQueriesFindOptJoinTree.Query2bFindOptJoinTree, 5228),
                Arguments.of(JOBQueriesFindOptJoinTree.Query2cFindOptJoinTree, 0),
                Arguments.of(JOBQueriesFindOptJoinTree.Query2dFindOptJoinTree, 68316),
                Arguments.of(JOBQueriesFindOptJoinTree.Query3aFindOptJoinTree, 206),
                Arguments.of(JOBQueriesFindOptJoinTree.Query3bFindOptJoinTree, 5),
                Arguments.of(JOBQueriesFindOptJoinTree.Query3cFindOptJoinTree, 7250),
                Arguments.of(JOBQueriesFindOptJoinTree.Query4aFindOptJoinTree, 740),
                Arguments.of(JOBQueriesFindOptJoinTree.Query4bFindOptJoinTree, 6),
                Arguments.of(JOBQueriesFindOptJoinTree.Query4cFindOptJoinTree, 4700),
                Arguments.of(JOBQueriesFindOptJoinTree.Query5aFindOptJoinTree, 0),
                Arguments.of(JOBQueriesFindOptJoinTree.Query5bFindOptJoinTree, 0),
                Arguments.of(JOBQueriesFindOptJoinTree.Query5cFindOptJoinTree, 669),
                Arguments.of(JOBQueriesFindOptJoinTree.Query6aFindOptJoinTree, 6),
                Arguments.of(JOBQueriesFindOptJoinTree.Query6bFindOptJoinTree, 12),
                Arguments.of(JOBQueriesFindOptJoinTree.Query6cFindOptJoinTree, 2),
                Arguments.of(JOBQueriesFindOptJoinTree.Query6dFindOptJoinTree, 88),
                Arguments.of(JOBQueriesFindOptJoinTree.Query6eFindOptJoinTree, 6),
                Arguments.of(JOBQueriesFindOptJoinTree.Query6fFindOptJoinTree, 785477),
                Arguments.of(JOBQueriesFindOptJoinTree.Query7aFindOptJoinTree, 32),
                Arguments.of(JOBQueriesFindOptJoinTree.Query7bFindOptJoinTree, 16),
                Arguments.of(JOBQueriesFindOptJoinTree.Query7cFindOptJoinTree, 68185),
                Arguments.of(JOBQueriesFindOptJoinTree.Query8aFindOptJoinTree, 62),
                Arguments.of(JOBQueriesFindOptJoinTree.Query8bFindOptJoinTree, 6),
                Arguments.of(JOBQueriesFindOptJoinTree.Query8cFindOptJoinTree, 2487611),
                Arguments.of(JOBQueriesFindOptJoinTree.Query8dFindOptJoinTree, 323005),
                Arguments.of(JOBQueriesFindOptJoinTree.Query9aFindOptJoinTree, 121),
                Arguments.of(JOBQueriesFindOptJoinTree.Query9bFindOptJoinTree, 40),
                Arguments.of(JOBQueriesFindOptJoinTree.Query9cFindOptJoinTree, 8144),
                Arguments.of(JOBQueriesFindOptJoinTree.Query9dFindOptJoinTree, 483082),
                Arguments.of(JOBQueriesFindOptJoinTree.Query10aFindOptJoinTree, 104),
                Arguments.of(JOBQueriesFindOptJoinTree.Query10bFindOptJoinTree, 0),
                Arguments.of(JOBQueriesFindOptJoinTree.Query10cFindOptJoinTree, 10),
                Arguments.of(JOBQueriesFindOptJoinTree.Query11aFindOptJoinTree, 310),
                Arguments.of(JOBQueriesFindOptJoinTree.Query11bFindOptJoinTree, 14),
                Arguments.of(JOBQueriesFindOptJoinTree.Query11cFindOptJoinTree, 6946),
                Arguments.of(JOBQueriesFindOptJoinTree.Query11dFindOptJoinTree, 14899),
                Arguments.of(JOBQueriesFindOptJoinTree.Query12aFindOptJoinTree, 397),
                Arguments.of(JOBQueriesFindOptJoinTree.Query12bFindOptJoinTree, 10),
                Arguments.of(JOBQueriesFindOptJoinTree.Query12cFindOptJoinTree, 4711),
                Arguments.of(JOBQueriesFindOptJoinTree.Query13aFindOptJoinTree, 111101),
                Arguments.of(JOBQueriesFindOptJoinTree.Query13bFindOptJoinTree, 372),
                Arguments.of(JOBQueriesFindOptJoinTree.Query13cFindOptJoinTree, 53),
                Arguments.of(JOBQueriesFindOptJoinTree.Query13dFindOptJoinTree, 670390),
                Arguments.of(JOBQueriesFindOptJoinTree.Query14aFindOptJoinTree, 761),
                Arguments.of(JOBQueriesFindOptJoinTree.Query14bFindOptJoinTree, 1),
                Arguments.of(JOBQueriesFindOptJoinTree.Query14cFindOptJoinTree, 4115),
                Arguments.of(JOBQueriesFindOptJoinTree.Query15aFindOptJoinTree, 328),
                Arguments.of(JOBQueriesFindOptJoinTree.Query15bFindOptJoinTree, 37),
                Arguments.of(JOBQueriesFindOptJoinTree.Query15cFindOptJoinTree, 2271),
                Arguments.of(JOBQueriesFindOptJoinTree.Query15dFindOptJoinTree, 11347),
                Arguments.of(JOBQueriesFindOptJoinTree.Query16aFindOptJoinTree, 385),
                Arguments.of(JOBQueriesFindOptJoinTree.Query16bFindOptJoinTree, 3710592),
                Arguments.of(JOBQueriesFindOptJoinTree.Query16cFindOptJoinTree, 319932),
                Arguments.of(JOBQueriesFindOptJoinTree.Query16dFindOptJoinTree, 249455),
                Arguments.of(JOBQueriesFindOptJoinTree.Query17aFindOptJoinTree, 258289),
                Arguments.of(JOBQueriesFindOptJoinTree.Query17bFindOptJoinTree, 52306),
                Arguments.of(JOBQueriesFindOptJoinTree.Query17cFindOptJoinTree, 1918),
                Arguments.of(JOBQueriesFindOptJoinTree.Query17dFindOptJoinTree, 11538),
                Arguments.of(JOBQueriesFindOptJoinTree.Query17eFindOptJoinTree, 2832555),
                Arguments.of(JOBQueriesFindOptJoinTree.Query17fFindOptJoinTree, 1113120),
                Arguments.of(JOBQueriesFindOptJoinTree.Query18aFindOptJoinTree, 410),
                Arguments.of(JOBQueriesFindOptJoinTree.Query18bFindOptJoinTree, 11),
                Arguments.of(JOBQueriesFindOptJoinTree.Query18cFindOptJoinTree, 28073),
                Arguments.of(JOBQueriesFindOptJoinTree.Query19aFindOptJoinTree, 184),
                Arguments.of(JOBQueriesFindOptJoinTree.Query19bFindOptJoinTree, 15),
                Arguments.of(JOBQueriesFindOptJoinTree.Query19cFindOptJoinTree, 3575),
                Arguments.of(JOBQueriesFindOptJoinTree.Query19dFindOptJoinTree, 1760645),
                Arguments.of(JOBQueriesFindOptJoinTree.Query20aFindOptJoinTree, 33),
                Arguments.of(JOBQueriesFindOptJoinTree.Query20bFindOptJoinTree, 17),
                Arguments.of(JOBQueriesFindOptJoinTree.Query20cFindOptJoinTree, 5406),
                Arguments.of(JOBQueriesFindOptJoinTree.Query21aFindOptJoinTree, 1410),
                Arguments.of(JOBQueriesFindOptJoinTree.Query21bFindOptJoinTree, 38),
                Arguments.of(JOBQueriesFindOptJoinTree.Query21cFindOptJoinTree, 1874),
                Arguments.of(JOBQueriesFindOptJoinTree.Query22aFindOptJoinTree, 2851),
                Arguments.of(JOBQueriesFindOptJoinTree.Query22bFindOptJoinTree, 31),
                Arguments.of(JOBQueriesFindOptJoinTree.Query22cFindOptJoinTree, 21489),
                Arguments.of(JOBQueriesFindOptJoinTree.Query22dFindOptJoinTree, 46281),
                Arguments.of(JOBQueriesFindOptJoinTree.Query23aFindOptJoinTree, 618),
                Arguments.of(JOBQueriesFindOptJoinTree.Query23bFindOptJoinTree, 16),
                Arguments.of(JOBQueriesFindOptJoinTree.Query23cFindOptJoinTree, 628),
                Arguments.of(JOBQueriesFindOptJoinTree.Query24aFindOptJoinTree, 275),
                Arguments.of(JOBQueriesFindOptJoinTree.Query24bFindOptJoinTree, 45),
                Arguments.of(JOBQueriesFindOptJoinTree.Query25aFindOptJoinTree, 4407),
                Arguments.of(JOBQueriesFindOptJoinTree.Query25bFindOptJoinTree, 6),
                Arguments.of(JOBQueriesFindOptJoinTree.Query25cFindOptJoinTree, 26153),
                Arguments.of(JOBQueriesFindOptJoinTree.Query26aFindOptJoinTree, 1728),
                Arguments.of(JOBQueriesFindOptJoinTree.Query26bFindOptJoinTree, 93),
                Arguments.of(JOBQueriesFindOptJoinTree.Query26cFindOptJoinTree, 5400),
                Arguments.of(JOBQueriesFindOptJoinTree.Query27aFindOptJoinTree, 477),
                Arguments.of(JOBQueriesFindOptJoinTree.Query27bFindOptJoinTree, 247),
                Arguments.of(JOBQueriesFindOptJoinTree.Query27cFindOptJoinTree, 743),
                Arguments.of(JOBQueriesFindOptJoinTree.Query28aFindOptJoinTree, 4803),
                Arguments.of(JOBQueriesFindOptJoinTree.Query28bFindOptJoinTree, 148),
                Arguments.of(JOBQueriesFindOptJoinTree.Query28cFindOptJoinTree, 8373),
                Arguments.of(JOBQueriesFindOptJoinTree.Query29aFindOptJoinTree, 1620),
                Arguments.of(JOBQueriesFindOptJoinTree.Query29bFindOptJoinTree, 15),
                Arguments.of(JOBQueriesFindOptJoinTree.Query29cFindOptJoinTree, 16308),
                Arguments.of(JOBQueriesFindOptJoinTree.Query30aFindOptJoinTree, 757),
                Arguments.of(JOBQueriesFindOptJoinTree.Query30bFindOptJoinTree, 28),
                Arguments.of(JOBQueriesFindOptJoinTree.Query30cFindOptJoinTree, 8024),
                Arguments.of(JOBQueriesFindOptJoinTree.Query31aFindOptJoinTree, 1273),
                Arguments.of(JOBQueriesFindOptJoinTree.Query31bFindOptJoinTree, 84),
                Arguments.of(JOBQueriesFindOptJoinTree.Query31cFindOptJoinTree, 2825),
                Arguments.of(JOBQueriesFindOptJoinTree.Query32aFindOptJoinTree, 0),
                Arguments.of(JOBQueriesFindOptJoinTree.Query32bFindOptJoinTree, 4388),
                Arguments.of(JOBQueriesFindOptJoinTree.Query33aFindOptJoinTree, 8),
                Arguments.of(JOBQueriesFindOptJoinTree.Query33bFindOptJoinTree, 4),
                Arguments.of(JOBQueriesFindOptJoinTree.Query33cFindOptJoinTree, 114)
        );
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessDataProvider")
    public void testCheckQueryImplementationCorrectness(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJBushy(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
        List<TableNode> outerTables = new ArrayList<>();
        gatherOuterTables(query.getPlan().getRoot(), outerTables);
        for (TableNode leftMostNode : outerTables) {
            assertEquals(SingleValueJavIntRowNoGoodListMap.class, leftMostNode.getOperator().getNoGoodListMap().getClass());
        }
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJNoDPBushy(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_NO_DP, queries,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJNoNGBushy(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_NO_NG, queries,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJVanillaBushy(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_VANILLA, queries,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    private Stream<Arguments> testCheckQueryImplementationCorrectnessTTJDataProvider()
    {
        return Stream.of(
                Arguments.of(JOBQueriesShallowHJOrdering.Query1aOptJoinTreeOptOrderingShallowHJOrdering, 142),
                Arguments.of(JOBQueriesShallowHJOrdering.Query1bOptJoinTreeOptOrderingShallowHJOrdering, 3),
                Arguments.of(JOBQueriesShallowHJOrdering.Query1cOptJoinTreeOptOrderingShallowHJOrdering, 3),
                Arguments.of(JOBQueriesShallowHJOrdering.Query1dOptJoinTreeOptOrderingShallowHJOrdering, 4),
                Arguments.of(JOBQueriesShallowHJOrdering.Query2aOptJoinTreeOptOrderingShallowHJOrdering, 7834),
                Arguments.of(JOBQueriesShallowHJOrdering.Query2bOptJoinTreeOptOrderingShallowHJOrdering, 5228),
                Arguments.of(JOBQueriesShallowHJOrdering.Query2cOptJoinTreeOptOrderingShallowHJOrdering, 0),
                Arguments.of(JOBQueriesShallowHJOrdering.Query2dOptJoinTreeOptOrderingShallowHJOrdering, 68316),
                Arguments.of(JOBQueriesShallowHJOrdering.Query3aOptJoinTreeOptOrderingShallowHJOrdering, 206),
                Arguments.of(JOBQueriesShallowHJOrdering.Query3bOptJoinTreeOptOrderingShallowHJOrdering, 5),
                Arguments.of(JOBQueriesShallowHJOrdering.Query3cOptJoinTreeOptOrderingShallowHJOrdering, 7250),
                Arguments.of(JOBQueriesShallowHJOrdering.Query4aOptJoinTreeOptOrderingShallowHJOrdering, 740),
                Arguments.of(JOBQueriesShallowHJOrdering.Query4bOptJoinTreeOptOrderingShallowHJOrdering, 6),
                Arguments.of(JOBQueriesShallowHJOrdering.Query4cOptJoinTreeOptOrderingShallowHJOrdering, 4700),
                Arguments.of(JOBQueriesShallowHJOrdering.Query5aOptJoinTreeOptOrderingShallowHJOrdering, 0),
                Arguments.of(JOBQueriesShallowHJOrdering.Query5bOptJoinTreeOptOrderingShallowHJOrdering, 0),
                Arguments.of(JOBQueriesShallowHJOrdering.Query5cOptJoinTreeOptOrderingShallowHJOrdering, 669),
                Arguments.of(JOBQueriesShallowHJOrdering.Query6aOptJoinTreeOptOrderingShallowHJOrdering, 6),
                Arguments.of(JOBQueriesShallowHJOrdering.Query6bOptJoinTreeOptOrderingShallowHJOrdering, 12),
                Arguments.of(JOBQueriesShallowHJOrdering.Query6cOptJoinTreeOptOrderingShallowHJOrdering, 2),
                Arguments.of(JOBQueriesShallowHJOrdering.Query6dOptJoinTreeOptOrderingShallowHJOrdering, 88),
                Arguments.of(JOBQueriesShallowHJOrdering.Query6eOptJoinTreeOptOrderingShallowHJOrdering, 6),
                Arguments.of(JOBQueriesShallowHJOrdering.Query6fOptJoinTreeOptOrderingShallowHJOrdering, 785477),
                Arguments.of(JOBQueriesShallowHJOrdering.Query7aOptJoinTreeOptOrderingShallowHJOrdering, 32),
                Arguments.of(JOBQueriesShallowHJOrdering.Query7bOptJoinTreeOptOrderingShallowHJOrdering, 16),
                Arguments.of(JOBQueriesShallowHJOrdering.Query7cOptJoinTreeOptOrderingShallowHJOrdering, 68185),
                Arguments.of(JOBQueriesShallowHJOrdering.Query8aOptJoinTreeOptOrderingShallowHJOrdering, 62),
                Arguments.of(JOBQueriesShallowHJOrdering.Query8bOptJoinTreeOptOrderingShallowHJOrdering, 6),
                Arguments.of(JOBQueriesShallowHJOrdering.Query8cOptJoinTreeOptOrderingShallowHJOrdering, 2487611),
                Arguments.of(JOBQueriesShallowHJOrdering.Query8dOptJoinTreeOptOrderingShallowHJOrdering, 323005),
                Arguments.of(JOBQueriesShallowHJOrdering.Query9aOptJoinTreeOptOrderingShallowHJOrdering, 121),
                Arguments.of(JOBQueriesShallowHJOrdering.Query9bOptJoinTreeOptOrderingShallowHJOrdering, 40),
                Arguments.of(JOBQueriesShallowHJOrdering.Query9cOptJoinTreeOptOrderingShallowHJOrdering, 8144),
                Arguments.of(JOBQueriesShallowHJOrdering.Query9dOptJoinTreeOptOrderingShallowHJOrdering, 483082),
                Arguments.of(JOBQueriesShallowHJOrdering.Query10aOptJoinTreeOptOrderingShallowHJOrdering, 104),
                Arguments.of(JOBQueriesShallowHJOrdering.Query10bOptJoinTreeOptOrderingShallowHJOrdering, 0),
                Arguments.of(JOBQueriesShallowHJOrdering.Query10cOptJoinTreeOptOrderingShallowHJOrdering, 10),
                Arguments.of(JOBQueriesShallowHJOrdering.Query11aOptJoinTreeOptOrderingShallowHJOrdering, 310),
                Arguments.of(JOBQueriesShallowHJOrdering.Query11bOptJoinTreeOptOrderingShallowHJOrdering, 14),
                Arguments.of(JOBQueriesShallowHJOrdering.Query11cOptJoinTreeOptOrderingShallowHJOrdering, 6946),
                Arguments.of(JOBQueriesShallowHJOrdering.Query11dOptJoinTreeOptOrderingShallowHJOrdering, 14899),
                Arguments.of(JOBQueriesShallowHJOrdering.Query12aOptJoinTreeOptOrderingShallowHJOrdering, 397),
                Arguments.of(JOBQueriesShallowHJOrdering.Query12bOptJoinTreeOptOrderingShallowHJOrdering, 10),
                Arguments.of(JOBQueriesShallowHJOrdering.Query12cOptJoinTreeOptOrderingShallowHJOrdering, 4711),
                Arguments.of(JOBQueriesShallowHJOrdering.Query13aOptJoinTreeOptOrderingShallowHJOrdering, 111101),
                Arguments.of(JOBQueriesShallowHJOrdering.Query13bOptJoinTreeOptOrderingShallowHJOrdering, 372),
                Arguments.of(JOBQueriesShallowHJOrdering.Query13cOptJoinTreeOptOrderingShallowHJOrdering, 53),
                Arguments.of(JOBQueriesShallowHJOrdering.Query13dOptJoinTreeOptOrderingShallowHJOrdering, 670390),
                Arguments.of(JOBQueriesShallowHJOrdering.Query14aOptJoinTreeOptOrderingShallowHJOrdering, 761),
                Arguments.of(JOBQueriesShallowHJOrdering.Query14bOptJoinTreeOptOrderingShallowHJOrdering, 1),
                Arguments.of(JOBQueriesShallowHJOrdering.Query14cOptJoinTreeOptOrderingShallowHJOrdering, 4115),
                Arguments.of(JOBQueriesShallowHJOrdering.Query15aOptJoinTreeOptOrderingShallowHJOrdering, 328),
                Arguments.of(JOBQueriesShallowHJOrdering.Query15bOptJoinTreeOptOrderingShallowHJOrdering, 37),
                Arguments.of(JOBQueriesShallowHJOrdering.Query15cOptJoinTreeOptOrderingShallowHJOrdering, 2271),
                Arguments.of(JOBQueriesShallowHJOrdering.Query15dOptJoinTreeOptOrderingShallowHJOrdering, 11347),
                Arguments.of(JOBQueriesShallowHJOrdering.Query16aOptJoinTreeOptOrderingShallowHJOrdering, 385),
                Arguments.of(JOBQueriesShallowHJOrdering.Query16bOptJoinTreeOptOrderingShallowHJOrdering, 3710592),
                Arguments.of(JOBQueriesShallowHJOrdering.Query16cOptJoinTreeOptOrderingShallowHJOrdering, 319932),
                Arguments.of(JOBQueriesShallowHJOrdering.Query16dOptJoinTreeOptOrderingShallowHJOrdering, 249455),
                Arguments.of(JOBQueriesShallowHJOrdering.Query17aOptJoinTreeOptOrderingShallowHJOrdering, 258289),
                Arguments.of(JOBQueriesShallowHJOrdering.Query17bOptJoinTreeOptOrderingShallowHJOrdering, 52306),
                Arguments.of(JOBQueriesShallowHJOrdering.Query17cOptJoinTreeOptOrderingShallowHJOrdering, 1918),
                Arguments.of(JOBQueriesShallowHJOrdering.Query17dOptJoinTreeOptOrderingShallowHJOrdering, 11538),
                Arguments.of(JOBQueriesShallowHJOrdering.Query17eOptJoinTreeOptOrderingShallowHJOrdering, 2832555),
                Arguments.of(JOBQueriesShallowHJOrdering.Query17fOptJoinTreeOptOrderingShallowHJOrdering, 1113120),
                Arguments.of(JOBQueriesShallowHJOrdering.Query18aOptJoinTreeOptOrderingShallowHJOrdering, 410),
                Arguments.of(JOBQueriesShallowHJOrdering.Query18bOptJoinTreeOptOrderingShallowHJOrdering, 11),
                Arguments.of(JOBQueriesShallowHJOrdering.Query18cOptJoinTreeOptOrderingShallowHJOrdering, 28073),
                Arguments.of(JOBQueriesShallowHJOrdering.Query19aOptJoinTreeOptOrderingShallowHJOrdering, 184),
                Arguments.of(JOBQueriesShallowHJOrdering.Query19bOptJoinTreeOptOrderingShallowHJOrdering, 15),
                Arguments.of(JOBQueriesShallowHJOrdering.Query19cOptJoinTreeOptOrderingShallowHJOrdering, 3575),
                Arguments.of(JOBQueriesShallowHJOrdering.Query19dOptJoinTreeOptOrderingShallowHJOrdering, 1760645),
                Arguments.of(JOBQueriesShallowHJOrdering.Query20aOptJoinTreeOptOrderingShallowHJOrdering, 33),
                Arguments.of(JOBQueriesShallowHJOrdering.Query20bOptJoinTreeOptOrderingShallowHJOrdering, 17),
                Arguments.of(JOBQueriesShallowHJOrdering.Query20cOptJoinTreeOptOrderingShallowHJOrdering, 5406),
                Arguments.of(JOBQueriesShallowHJOrdering.Query21aOptJoinTreeOptOrderingShallowHJOrdering, 1410),
                Arguments.of(JOBQueriesShallowHJOrdering.Query21bOptJoinTreeOptOrderingShallowHJOrdering, 38),
                Arguments.of(JOBQueriesShallowHJOrdering.Query21cOptJoinTreeOptOrderingShallowHJOrdering, 1874),
                Arguments.of(JOBQueriesShallowHJOrdering.Query22aOptJoinTreeOptOrderingShallowHJOrdering, 2851),
                Arguments.of(JOBQueriesShallowHJOrdering.Query22bOptJoinTreeOptOrderingShallowHJOrdering, 31),
                Arguments.of(JOBQueriesShallowHJOrdering.Query22cOptJoinTreeOptOrderingShallowHJOrdering, 21489),
                Arguments.of(JOBQueriesShallowHJOrdering.Query22dOptJoinTreeOptOrderingShallowHJOrdering, 46281),
                Arguments.of(JOBQueriesShallowHJOrdering.Query23aOptJoinTreeOptOrderingShallowHJOrdering, 618),
                Arguments.of(JOBQueriesShallowHJOrdering.Query23bOptJoinTreeOptOrderingShallowHJOrdering, 16),
                Arguments.of(JOBQueriesShallowHJOrdering.Query23cOptJoinTreeOptOrderingShallowHJOrdering, 628),
                Arguments.of(JOBQueriesShallowHJOrdering.Query24aOptJoinTreeOptOrderingShallowHJOrdering, 275),
                Arguments.of(JOBQueriesShallowHJOrdering.Query24bOptJoinTreeOptOrderingShallowHJOrdering, 45),
                Arguments.of(JOBQueriesShallowHJOrdering.Query25aOptJoinTreeOptOrderingShallowHJOrdering, 4407),
                Arguments.of(JOBQueriesShallowHJOrdering.Query25bOptJoinTreeOptOrderingShallowHJOrdering, 6),
                Arguments.of(JOBQueriesShallowHJOrdering.Query25cOptJoinTreeOptOrderingShallowHJOrdering, 26153),
                Arguments.of(JOBQueriesShallowHJOrdering.Query26aOptJoinTreeOptOrderingShallowHJOrdering, 1728),
                Arguments.of(JOBQueriesShallowHJOrdering.Query26bOptJoinTreeOptOrderingShallowHJOrdering, 93),
                Arguments.of(JOBQueriesShallowHJOrdering.Query26cOptJoinTreeOptOrderingShallowHJOrdering, 5400),
                Arguments.of(JOBQueriesShallowHJOrdering.Query27aOptJoinTreeOptOrderingShallowHJOrdering, 477),
                Arguments.of(JOBQueriesShallowHJOrdering.Query27bOptJoinTreeOptOrderingShallowHJOrdering, 247),
                Arguments.of(JOBQueriesShallowHJOrdering.Query27cOptJoinTreeOptOrderingShallowHJOrdering, 743),
                Arguments.of(JOBQueriesShallowHJOrdering.Query28aOptJoinTreeOptOrderingShallowHJOrdering, 4803),
                Arguments.of(JOBQueriesShallowHJOrdering.Query28bOptJoinTreeOptOrderingShallowHJOrdering, 148),
                Arguments.of(JOBQueriesShallowHJOrdering.Query28cOptJoinTreeOptOrderingShallowHJOrdering, 8373),
                Arguments.of(JOBQueriesShallowHJOrdering.Query29aOptJoinTreeOptOrderingShallowHJOrdering, 1620),
                Arguments.of(JOBQueriesShallowHJOrdering.Query29bOptJoinTreeOptOrderingShallowHJOrdering, 15),
                Arguments.of(JOBQueriesShallowHJOrdering.Query29cOptJoinTreeOptOrderingShallowHJOrdering, 16308),
                Arguments.of(JOBQueriesShallowHJOrdering.Query30aOptJoinTreeOptOrderingShallowHJOrdering, 757),
                Arguments.of(JOBQueriesShallowHJOrdering.Query30bOptJoinTreeOptOrderingShallowHJOrdering, 28),
                Arguments.of(JOBQueriesShallowHJOrdering.Query30cOptJoinTreeOptOrderingShallowHJOrdering, 8024),
                Arguments.of(JOBQueriesShallowHJOrdering.Query31aOptJoinTreeOptOrderingShallowHJOrdering, 1273),
                Arguments.of(JOBQueriesShallowHJOrdering.Query31bOptJoinTreeOptOrderingShallowHJOrdering, 84),
                Arguments.of(JOBQueriesShallowHJOrdering.Query31cOptJoinTreeOptOrderingShallowHJOrdering, 2825),
                Arguments.of(JOBQueriesShallowHJOrdering.Query32aOptJoinTreeOptOrderingShallowHJOrdering, 0),
                Arguments.of(JOBQueriesShallowHJOrdering.Query32bOptJoinTreeOptOrderingShallowHJOrdering, 4388),
                Arguments.of(JOBQueriesShallowHJOrdering.Query33aOptJoinTreeOptOrderingShallowHJOrdering, 8),
                Arguments.of(JOBQueriesShallowHJOrdering.Query33bOptJoinTreeOptOrderingShallowHJOrdering, 4),
                Arguments.of(JOBQueriesShallowHJOrdering.Query33cOptJoinTreeOptOrderingShallowHJOrdering, 114)
        );
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessTTJDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJ(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
        PlanNode leftMostNode = getLeftMostNode(query.getPlan().getRoot());
        assertEquals(SingleValueJavIntRowNoGoodListMap.class, leftMostNode.getOperator().getNoGoodListMap().getClass());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessTTJDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJNoDP(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_NO_DP, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessTTJDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJVanilla(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_VANILLA, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessTTJDataProvider")
    public void testCheckQueryImplementationCorrectnessHJ(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    private Stream<Arguments> testCheckQueryImplementationCorrectnessYA1PassDataProvider()
    {
        return Stream.of(
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query1aOptJoinTreeOptOrderingY1PShallowHJOrdering, 142),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query1bOptJoinTreeOptOrderingY1PShallowHJOrdering, 3),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query1cOptJoinTreeOptOrderingY1PShallowHJOrdering, 3),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query1dOptJoinTreeOptOrderingY1PShallowHJOrdering, 4),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query2aOptJoinTreeOptOrderingY1PShallowHJOrdering, 7834),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query2bOptJoinTreeOptOrderingY1PShallowHJOrdering, 5228),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query2cOptJoinTreeOptOrderingY1PShallowHJOrdering, 0),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query2dOptJoinTreeOptOrderingY1PShallowHJOrdering, 68316),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query3aOptJoinTreeOptOrderingY1PShallowHJOrdering, 206),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query3bOptJoinTreeOptOrderingY1PShallowHJOrdering, 5),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query3cOptJoinTreeOptOrderingY1PShallowHJOrdering, 7250),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query4aOptJoinTreeOptOrderingY1PShallowHJOrdering, 740),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query4bOptJoinTreeOptOrderingY1PShallowHJOrdering, 6),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query4cOptJoinTreeOptOrderingY1PShallowHJOrdering, 4700),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query5aOptJoinTreeOptOrderingY1PShallowHJOrdering, 0),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query5bOptJoinTreeOptOrderingY1PShallowHJOrdering, 0),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query5cOptJoinTreeOptOrderingY1PShallowHJOrdering, 669),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query6aOptJoinTreeOptOrderingY1PShallowHJOrdering, 6),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query6bOptJoinTreeOptOrderingY1PShallowHJOrdering, 12),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query6cOptJoinTreeOptOrderingY1PShallowHJOrdering, 2),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query6dOptJoinTreeOptOrderingY1PShallowHJOrdering, 88),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query6eOptJoinTreeOptOrderingY1PShallowHJOrdering, 6),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query6fOptJoinTreeOptOrderingY1PShallowHJOrdering, 785477),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query7aOptJoinTreeOptOrderingY1PShallowHJOrdering, 32),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query7bOptJoinTreeOptOrderingY1PShallowHJOrdering, 16),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query7cOptJoinTreeOptOrderingY1PShallowHJOrdering, 68185),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query8aOptJoinTreeOptOrderingY1PShallowHJOrdering, 62),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query8bOptJoinTreeOptOrderingY1PShallowHJOrdering, 6),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query8cOptJoinTreeOptOrderingY1PShallowHJOrdering, 2487611),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query8dOptJoinTreeOptOrderingY1PShallowHJOrdering, 323005),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query9aOptJoinTreeOptOrderingY1PShallowHJOrdering, 121),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query9bOptJoinTreeOptOrderingY1PShallowHJOrdering, 40),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query9cOptJoinTreeOptOrderingY1PShallowHJOrdering, 8144),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query9dOptJoinTreeOptOrderingY1PShallowHJOrdering, 483082),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query10aOptJoinTreeOptOrderingY1PShallowHJOrdering, 104),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query10bOptJoinTreeOptOrderingY1PShallowHJOrdering, 0),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query10cOptJoinTreeOptOrderingY1PShallowHJOrdering, 10),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query11aOptJoinTreeOptOrderingY1PShallowHJOrdering, 310),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query11bOptJoinTreeOptOrderingY1PShallowHJOrdering, 14),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query11cOptJoinTreeOptOrderingY1PShallowHJOrdering, 6946),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query11dOptJoinTreeOptOrderingY1PShallowHJOrdering, 14899),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query12aOptJoinTreeOptOrderingY1PShallowHJOrdering, 397),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query12bOptJoinTreeOptOrderingY1PShallowHJOrdering, 10),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query12cOptJoinTreeOptOrderingY1PShallowHJOrdering, 4711),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query13aOptJoinTreeOptOrderingY1PShallowHJOrdering, 111101),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query13bOptJoinTreeOptOrderingY1PShallowHJOrdering, 372),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query13cOptJoinTreeOptOrderingY1PShallowHJOrdering, 53),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query13dOptJoinTreeOptOrderingY1PShallowHJOrdering, 670390),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query14aOptJoinTreeOptOrderingY1PShallowHJOrdering, 761),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query14bOptJoinTreeOptOrderingY1PShallowHJOrdering, 1),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query14cOptJoinTreeOptOrderingY1PShallowHJOrdering, 4115),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query15aOptJoinTreeOptOrderingY1PShallowHJOrdering, 328),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query15bOptJoinTreeOptOrderingY1PShallowHJOrdering, 37),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query15cOptJoinTreeOptOrderingY1PShallowHJOrdering, 2271),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query15dOptJoinTreeOptOrderingY1PShallowHJOrdering, 11347),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query16aOptJoinTreeOptOrderingY1PShallowHJOrdering, 385),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query16bOptJoinTreeOptOrderingY1PShallowHJOrdering, 3710592),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query16cOptJoinTreeOptOrderingY1PShallowHJOrdering, 319932),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query16dOptJoinTreeOptOrderingY1PShallowHJOrdering, 249455),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query17aOptJoinTreeOptOrderingY1PShallowHJOrdering, 258289),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query17bOptJoinTreeOptOrderingY1PShallowHJOrdering, 52306),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query17cOptJoinTreeOptOrderingY1PShallowHJOrdering, 1918),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query17dOptJoinTreeOptOrderingY1PShallowHJOrdering, 11538),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query17eOptJoinTreeOptOrderingY1PShallowHJOrdering, 2832555),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query17fOptJoinTreeOptOrderingY1PShallowHJOrdering, 1113120),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query18aOptJoinTreeOptOrderingY1PShallowHJOrdering, 410),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query18bOptJoinTreeOptOrderingY1PShallowHJOrdering, 11),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query18cOptJoinTreeOptOrderingY1PShallowHJOrdering, 28073),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query19aOptJoinTreeOptOrderingY1PShallowHJOrdering, 184),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query19bOptJoinTreeOptOrderingY1PShallowHJOrdering, 15),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query19cOptJoinTreeOptOrderingY1PShallowHJOrdering, 3575),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query19dOptJoinTreeOptOrderingY1PShallowHJOrdering, 1760645),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query20aOptJoinTreeOptOrderingY1PShallowHJOrdering, 33),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query20bOptJoinTreeOptOrderingY1PShallowHJOrdering, 17),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query20cOptJoinTreeOptOrderingY1PShallowHJOrdering, 5406),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query21aOptJoinTreeOptOrderingY1PShallowHJOrdering, 1410),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query21bOptJoinTreeOptOrderingY1PShallowHJOrdering, 38),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query21cOptJoinTreeOptOrderingY1PShallowHJOrdering, 1874),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query22aOptJoinTreeOptOrderingY1PShallowHJOrdering, 2851),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query22bOptJoinTreeOptOrderingY1PShallowHJOrdering, 31),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query22cOptJoinTreeOptOrderingY1PShallowHJOrdering, 21489),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query22dOptJoinTreeOptOrderingY1PShallowHJOrdering, 46281),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query23aOptJoinTreeOptOrderingY1PShallowHJOrdering, 618),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query23bOptJoinTreeOptOrderingY1PShallowHJOrdering, 16),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query23cOptJoinTreeOptOrderingY1PShallowHJOrdering, 628),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query24aOptJoinTreeOptOrderingY1PShallowHJOrdering, 275),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query24bOptJoinTreeOptOrderingY1PShallowHJOrdering, 45),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query25aOptJoinTreeOptOrderingY1PShallowHJOrdering, 4407),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query25bOptJoinTreeOptOrderingY1PShallowHJOrdering, 6),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query25cOptJoinTreeOptOrderingY1PShallowHJOrdering, 26153),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query26aOptJoinTreeOptOrderingY1PShallowHJOrdering, 1728),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query26bOptJoinTreeOptOrderingY1PShallowHJOrdering, 93),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query26cOptJoinTreeOptOrderingY1PShallowHJOrdering, 5400),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query27aOptJoinTreeOptOrderingY1PShallowHJOrdering, 477),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query27bOptJoinTreeOptOrderingY1PShallowHJOrdering, 247),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query27cOptJoinTreeOptOrderingY1PShallowHJOrdering, 743),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query28aOptJoinTreeOptOrderingY1PShallowHJOrdering, 4803),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query28bOptJoinTreeOptOrderingY1PShallowHJOrdering, 148),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query28cOptJoinTreeOptOrderingY1PShallowHJOrdering, 8373),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query29aOptJoinTreeOptOrderingY1PShallowHJOrdering, 1620),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query29bOptJoinTreeOptOrderingY1PShallowHJOrdering, 15),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query29cOptJoinTreeOptOrderingY1PShallowHJOrdering, 16308),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query30aOptJoinTreeOptOrderingY1PShallowHJOrdering, 757),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query30bOptJoinTreeOptOrderingY1PShallowHJOrdering, 28),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query30cOptJoinTreeOptOrderingY1PShallowHJOrdering, 8024),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query31aOptJoinTreeOptOrderingY1PShallowHJOrdering, 1273),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query31bOptJoinTreeOptOrderingY1PShallowHJOrdering, 84),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query31cOptJoinTreeOptOrderingY1PShallowHJOrdering, 2825),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query32aOptJoinTreeOptOrderingY1PShallowHJOrdering, 0),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query32bOptJoinTreeOptOrderingY1PShallowHJOrdering, 4388),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query33aOptJoinTreeOptOrderingY1PShallowHJOrdering, 8),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query33bOptJoinTreeOptOrderingY1PShallowHJOrdering, 4),
                Arguments.of(JOBQueriesYannakakis1PassShallow.Query33cOptJoinTreeOptOrderingY1PShallowHJOrdering, 114)
        );
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessYA1PassDataProvider")
    public void testCheckQueryImplementationCorrectnessYA1Pass(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.Yannakakis1Pass, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    private Stream<Arguments> testCheckQueryImplementationCorrectnessTPCHDataProvider()
    {
        return Stream.of(
                Arguments.of(TPCHQueriesFindOptJoinTree.Query3WFindOptJoinTree, 30519),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query7aWFindOptJoinTree, 3014),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query7bWFindOptJoinTree, 2910),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query8WFindOptJoinTree, 2603),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query9WFindOptJoinTree, 319404),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query10WFindOptJoinTree, 114705),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query11WFindOptJoinTree, 31680),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query12WFindOptJoinTree, 30988),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query14WFindOptJoinTree, 75983),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query15WFindOptJoinTree, 1),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query16WFindOptJoinTree, 118274),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query18WFindOptJoinTree, 399),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query19aWFindOptJoinTree, 25),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query19bWFindOptJoinTree, 40),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query19cWFindOptJoinTree, 56),
                Arguments.of(TPCHQueriesFindOptJoinTree.Query20WFindOptJoinTree, 198)
        );
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessTPCHDataProvider")
    public void testCheckQueryImplementationCorrectnessTPCH(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    private Stream<Arguments> testCheckQueryImplementationCorrectnessTPCHDataTTJProvider()
    {
        return Stream.of(
                Arguments.of(TPCHQueriesShallowHJOrdering.Query3WOptJoinTreeOptOrderingShallowHJOrdering, 30519),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query7aWOptJoinTreeOptOrderingShallowHJOrdering, 3014),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query7bWOptJoinTreeOptOrderingShallowHJOrdering, 2910),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query8WOptJoinTreeOptOrderingShallowHJOrdering, 2603),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query9WOptJoinTreeOptOrderingShallowHJOrdering, 319404),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query10WOptJoinTreeOptOrderingShallowHJOrdering, 114705),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query11WOptJoinTreeOptOrderingShallowHJOrdering, 31680),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query12WOptJoinTreeOptOrderingShallowHJOrdering, 30988),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query14WOptJoinTreeOptOrderingShallowHJOrdering, 75983),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query15WOptJoinTreeOptOrderingShallowHJOrdering, 1),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query16WOptJoinTreeOptOrderingShallowHJOrdering, 118274),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query18WOptJoinTreeOptOrderingShallowHJOrdering, 399),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query19aWOptJoinTreeOptOrderingShallowHJOrdering, 25),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query19bWOptJoinTreeOptOrderingShallowHJOrdering, 40),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query19cWOptJoinTreeOptOrderingShallowHJOrdering, 56),
                Arguments.of(TPCHQueriesShallowHJOrdering.Query20WOptJoinTreeOptOrderingShallowHJOrdering, 198)
        );
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessTPCHDataTTJProvider")
    public void testCheckQueryImplementationCorrectnessTPCHTTJNoDP(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_NO_DP, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    private Stream<Arguments> testCheckQueryImplementationCorrectnessSSBDataTTJProvider()
    {
        return Stream.of(
                Arguments.of(SSBQueries.Q1P1, 118735),
                Arguments.of(SSBQueries.Q1P2, 4251),
                Arguments.of(SSBQueries.Q1P3, 470),
                Arguments.of(SSBQueries.Q2P1, 46026),
                Arguments.of(SSBQueries.Q2P2, 10577),
                Arguments.of(SSBQueries.Q2P3, 1222),
                Arguments.of(SSBQueries.Q3P1, 246821),
                Arguments.of(SSBQueries.Q3P2, 8606),
                Arguments.of(SSBQueries.Q3P3, 339),
                Arguments.of(SSBQueries.Q3P4, 5),
                Arguments.of(SSBQueries.Q4P1, 90353),
                Arguments.of(SSBQueries.Q4P2, 21803),
                Arguments.of(SSBQueries.Q4P3, 99)
        );
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessSSBDataTTJProvider")
    public void testCheckQueryImplementationCorrectnessSSBTTJHP(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    @ParameterizedTest
    @MethodSource("testCheckQueryImplementationCorrectnessSSBDataTTJProvider")
    public void testCheckQueryImplementationCorrectnessSSBYannakakis1Pass(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.Yannakakis1Pass, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }
}
