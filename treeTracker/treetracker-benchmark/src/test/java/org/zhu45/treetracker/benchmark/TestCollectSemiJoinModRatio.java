package org.zhu45.treetracker.benchmark;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.micro.exp2p9.Exp2P9Queries;

import java.util.HashMap;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.benchmark.statsgen.CollectSemiJoinModRatio.getSemiJoinModRatioResult;
import static org.zhu45.treetracker.common.TestConstants.Constants.GITHUB_VALUE;

@DisabledIfEnvironmentVariable(named = GITHUB_VALUE, matches = "1")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestCollectSemiJoinModRatio
{
    private Stream<Arguments> testCollectSemiJoinModRatioTestCasesDataProvider()
    {
        return Stream.of(Arguments.of(Exp2P9Queries.Exp2P9Query0P, (float) 0.1),
                Arguments.of(Exp2P9Queries.Exp2P9Query10P, (float) 0.1),
                Arguments.of(Exp2P9Queries.Exp2P9Query20P, (float) 0.2),
                Arguments.of(Exp2P9Queries.Exp2P9Query30P, (float) 0.3),
                Arguments.of(Exp2P9Queries.Exp2P9Query40P, (float) 0.4),
                Arguments.of(Exp2P9Queries.Exp2P9Query50P, (float) 0.5),
                Arguments.of(Exp2P9Queries.Exp2P9Query60P, (float) 0.6),
                Arguments.of(Exp2P9Queries.Exp2P9Query70P, (float) 0.7),
                Arguments.of(Exp2P9Queries.Exp2P9Query80P, (float) 0.8),
                Arguments.of(Exp2P9Queries.Exp2P9Query90P, (float) 0.9),
                Arguments.of(Exp2P9Queries.Exp2P9Query100P, (float) 1));
    }

    @ParameterizedTest
    @MethodSource("testCollectSemiJoinModRatioTestCasesDataProvider")
    public void testCollectSemiJoinModRatio(QueryEnum queryEnum, Float expectedSemiJoinModRatio)
    {
        HashMap<MultiwayJoinNode, Float> ratios = getSemiJoinModRatioResult(queryEnum);
        for (MultiwayJoinNode node : ratios.keySet()) {
            if (node.getSchemaTableName().getTableName().contains("u")) {
                assertEquals(expectedSemiJoinModRatio, ratios.get(node));
                break;
            }
        }
    }
}
