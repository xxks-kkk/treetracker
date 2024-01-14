package org.zhu45.treetracker.benchmark.tpch;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTPCHQueriesImpl
{
    private Stream<Arguments> testTestTPCHQueriesImplDataProvider()
    {
        return Stream.of(Arguments.of(JoinOperator.TTJHP, TPCHQueriesAutoGen.Query16WOptJoinTreeOptOrdering, 118274),
                Arguments.of(JoinOperator.Yannakakis, TPCHQueriesYannakakis.Query16WOptJoinTreeOptOrderingY, 118274),
                Arguments.of(JoinOperator.YannakakisB, TPCHQueriesYannakakisB.Query16WOptJoinTreeOptOrderingYB, 118274),
                Arguments.of(JoinOperator.TTJHP, TPCHQueriesAutoGen.Query19aWOptJoinTreeOptOrdering, 25),
                Arguments.of(JoinOperator.TTJHP, TPCHQueriesAutoGen.Query19bWOptJoinTreeOptOrdering, 40),
                Arguments.of(JoinOperator.TTJHP, TPCHQueriesAutoGen.Query19cWOptJoinTreeOptOrdering, 56),
                Arguments.of(JoinOperator.Yannakakis, TPCHQueriesYannakakis.Query19aWOptJoinTreeOptOrderingY, 25),
                Arguments.of(JoinOperator.Yannakakis, TPCHQueriesYannakakis.Query19bWOptJoinTreeOptOrderingY, 40),
                Arguments.of(JoinOperator.Yannakakis, TPCHQueriesYannakakis.Query19cWOptJoinTreeOptOrderingY, 56),
                Arguments.of(JoinOperator.YannakakisB, TPCHQueriesYannakakisB.Query19aWOptJoinTreeOptOrderingYB, 25),
                Arguments.of(JoinOperator.YannakakisB, TPCHQueriesYannakakisB.Query19bWOptJoinTreeOptOrderingYB, 40),
                Arguments.of(JoinOperator.YannakakisB, TPCHQueriesYannakakisB.Query19cWOptJoinTreeOptOrderingYB, 56));
    }

    @ParameterizedTest
    @MethodSource("testTestTPCHQueriesImplDataProvider")
    public void testTPCHQueriesImpl(JoinOperator joinOperator, QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(joinOperator, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }
}
