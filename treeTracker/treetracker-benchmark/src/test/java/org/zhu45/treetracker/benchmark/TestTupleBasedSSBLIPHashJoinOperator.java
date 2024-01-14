package org.zhu45.treetracker.benchmark;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treetracker.benchmark.ssb.SSBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestTupleBasedSSBLIPHashJoinOperator
{
    private Stream<Arguments> testTupleBasedSSBLIPHashJoinOperatorDataProvider()
    {
        return Stream.of(
                Arguments.of(JoinOperator.LIP, SSBQueries.Q1P1, 118735),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q1P2, 4251),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q1P3, 470),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q2P1, 46026),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q2P2, 10577),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q2P3, 1222),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q3P1, 246821),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q3P2, 8606),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q3P3, 339),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q3P4, 5),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q4P1, 90353),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q4P2, 21803),
                Arguments.of(JoinOperator.LIP, SSBQueries.Q4P3, 99));
    }

    @ParameterizedTest
    @MethodSource("testTupleBasedSSBLIPHashJoinOperatorDataProvider")
    public void testYannakakisConfigCorrectness(JoinOperator joinOperator, QueryEnum queries, long expectedResultSize)
    {
        JoinFragmentType query = queryProvider(joinOperator, queries, duckDBJdbcClientSupplier.get());
        assertEquals(expectedResultSize, query.evalWithResultSize());
    }
}
