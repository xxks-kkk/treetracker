package org.zhu45.treetracker.benchmark.tpch;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavIntRowNoGoodListMap;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.DecideBackJumpingPointsForBushyPlan;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.gatherOuterTables;

@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTPCHQueriesImpl
{
    private Stream<Arguments> testTestTPCHQueriesImplDataProvider()
    {
        return Stream.of(
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query3WOptJoinTreeOptOrdering, 30519),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query7aWOptJoinTreeOptOrdering, 3014),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query7bWOptJoinTreeOptOrdering, 2910),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query8WOptJoinTreeOptOrdering, 2603),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query9WOptJoinTreeOptOrdering, 319404),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query10WOptJoinTreeOptOrdering, 114705),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query11WOptJoinTreeOptOrdering, 31680),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query12WOptJoinTreeOptOrdering, 30988),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query14WOptJoinTreeOptOrdering, 75983),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query15WOptJoinTreeOptOrdering, 1),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query16WOptJoinTreeOptOrdering, 118274),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query18WOptJoinTreeOptOrdering, 399),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query19aWOptJoinTreeOptOrdering, 25),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query19bWOptJoinTreeOptOrdering, 40),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query19cWOptJoinTreeOptOrdering, 56),
                Arguments.of(JoinOperator.TTJV2, TPCHQueriesAutoGen.Query20WOptJoinTreeOptOrdering, 198));
    }

    @ParameterizedTest
    @MethodSource("testTestTPCHQueriesImplDataProvider")
    public void testTPCHQueriesImpl(JoinOperator joinOperator, QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(joinOperator, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }

    private Stream<Arguments> testTPCHQueriesPostgresPlansDataProvider()
    {
        return Stream.of(Arguments.of(TPCHQueriesFindOptJoinTree.Query3WFindOptJoinTree, 30519),
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
                Arguments.of(TPCHQueriesFindOptJoinTree.Query20WFindOptJoinTree, 198));
    }

    @ParameterizedTest
    @MethodSource("testTPCHQueriesPostgresPlansDataProvider")
    public void testCheckQueryImplementationCorrectnessTTJBushy(QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries,
                List.of(new DecideBackJumpingPointsForBushyPlan()),
                duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
        List<TableNode> outerTables = new ArrayList<>();
        gatherOuterTables(query.getPlan().getRoot(), outerTables);
        for (TableNode leftMostNode : outerTables) {
            try {
                assertEquals(SingleValueJavIntRowNoGoodListMap.class, leftMostNode.getOperator().getNoGoodListMap().getClass());
            }
            catch (AssertionFailedError e) {
                assertEquals(TPCHQueriesFindOptJoinTree.Query9WFindOptJoinTree.getQueryClazz(), queries.getQueryClazz());
                assertTrue(leftMostNode.getSchemaTableName().getTableName().equals("partsupp"));
            }
        }
    }
}
