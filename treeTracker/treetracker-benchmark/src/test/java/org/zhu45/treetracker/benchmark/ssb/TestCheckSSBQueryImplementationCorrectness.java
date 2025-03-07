package org.zhu45.treetracker.benchmark.ssb;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.gatherOuterTables;

@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCheckSSBQueryImplementationCorrectness
{
    private Stream<Arguments> testCheckQueryImplementationCorrectnessDataProvider()
    {
        return Stream.of(
                Arguments.of(SSBQueriesFindOptJoinTree.Query1P1FindOptJoinTree, 118735),
                Arguments.of(SSBQueriesFindOptJoinTree.Query1P2FindOptJoinTree, 4251),
                Arguments.of(SSBQueriesFindOptJoinTree.Query1P3FindOptJoinTree, 470),
                Arguments.of(SSBQueriesFindOptJoinTree.Query2P1FindOptJoinTree, 46026),
                Arguments.of(SSBQueriesFindOptJoinTree.Query2P2FindOptJoinTree, 10577),
                Arguments.of(SSBQueriesFindOptJoinTree.Query2P3FindOptJoinTree, 1222),
                Arguments.of(SSBQueriesFindOptJoinTree.Query3P1FindOptJoinTree, 246821),
                Arguments.of(SSBQueriesFindOptJoinTree.Query3P2FindOptJoinTree, 8606),
                Arguments.of(SSBQueriesFindOptJoinTree.Query3P3FindOptJoinTree, 339),
                Arguments.of(SSBQueriesFindOptJoinTree.Query3P4FindOptJoinTree, 5),
                Arguments.of(SSBQueriesFindOptJoinTree.Query4P1FindOptJoinTree, 90353),
                Arguments.of(SSBQueriesFindOptJoinTree.Query4P2FindOptJoinTree, 21803),
                Arguments.of(SSBQueriesFindOptJoinTree.Query4P3FindOptJoinTree, 99)
        );
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
}
