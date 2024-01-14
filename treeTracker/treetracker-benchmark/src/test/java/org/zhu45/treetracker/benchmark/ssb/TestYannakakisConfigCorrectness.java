package org.zhu45.treetracker.benchmark.ssb;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.JOBQueriesYannakakis;
import org.zhu45.treetracker.benchmark.job.JOBQueriesYannakakisB;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.FullReducerOperator;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

/**
 * Test Yannakakis family of algorithms are correctly configured
 */
@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestYannakakisConfigCorrectness
{
    private Stream<Arguments> testYannakakisConfigCorrectnessDataProvider()
    {
        return Stream.of(
//                Arguments.of(JoinOperator.YannakakisVanilla, TPCHQueriesYannakakisV.Query12WOptJoinTreeOptOrderingYV, 30988),
                Arguments.of(JoinOperator.YannakakisB, JOBQueriesYannakakisB.Query6aOptJoinTreeOptOrderingYB, 6),
                Arguments.of(JoinOperator.Yannakakis, JOBQueriesYannakakis.Query6aOptJoinTreeOptOrderingY, 6));
    }

    @ParameterizedTest
    @MethodSource("testYannakakisConfigCorrectnessDataProvider")
    public void testYannakakisConfigCorrectness(JoinOperator joinOperator, QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(joinOperator, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
        Operator operator = query.getPlan().getRoot().getOperator();
        assertEquals(FullReducerOperator.class, operator.getClass());
        FullReducerOperator fullReducerOperator = (FullReducerOperator) operator;
        if (joinOperator == JoinOperator.YannakakisB) {
            PlanBuildContext context = query.getPlan().getRoot().getOperator().getPlanBuildContext();
            assertEquals(TupleBasedLeftSemiBloomJoinOperator.class, context.getSemiJoinClazz());
            assertEquals(false, context.getDisablePTOptimizationTrick());
            assertEquals(TupleBasedLeftSemiBloomJoinOperator.class, fullReducerOperator.getBottomUpSemijoins().get(0).getRoot().getOperator().getClass());
        }
        if (joinOperator == JoinOperator.Yannakakis) {
            PlanBuildContext context = query.getPlan().getRoot().getOperator().getPlanBuildContext();
            assertEquals(TupleBasedLeftSemiHashJoinOperator.class, context.getSemiJoinClazz());
            assertEquals(false, context.getDisablePTOptimizationTrick());
            assertEquals(TupleBasedLeftSemiHashJoinOperator.class, fullReducerOperator.getBottomUpSemijoins().get(0).getRoot().getOperator().getClass());
        }
        if (joinOperator == JoinOperator.YannakakisVanilla) {
            PlanBuildContext context = query.getPlan().getRoot().getOperator().getPlanBuildContext();
            assertEquals(TupleBasedLeftSemiHashJoinOperator.class, context.getSemiJoinClazz());
            assertEquals(true, context.getDisablePTOptimizationTrick());
            assertEquals(TupleBasedLeftSemiHashJoinOperator.class, fullReducerOperator.getBottomUpSemijoins().get(0).getRoot().getOperator().getClass());
        }
    }
}
