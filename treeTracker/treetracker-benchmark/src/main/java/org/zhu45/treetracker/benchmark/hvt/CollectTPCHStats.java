package org.zhu45.treetracker.benchmark.hvt;

import de.renebergelt.test.Switches;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesAutoGen;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesFindOptJoinTree;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesHashJoinOnYannakakis;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesYannakakis;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesYannakakisB;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class CollectTPCHStats
{
    private CollectTPCHStats(){}

    private static void run(JoinOperator algorithm, Class<? extends Query> queryClazz)
    {
        System.out.println("JoinOperator: " + algorithm);
        System.out.println("queryClazz: " + queryClazz);
        checkState(Switches.STATS, "need to enable Switches.STATS");
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(algorithm)
                .setQueryClazz(queryClazz)
                .setRules(List.of())
                .setJdbcClient(duckDBJdbcClientSupplier.get())
                .build();
        JoinFragmentType query = queryProvider(context);
        createStatisticsJson(query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
    }

    private static void runHelper(JoinOperator joinOperator,
                                  Class<? extends QueryEnum> queryEnumClazz)
    {
        List<Class<? extends Query>> queryEnums = Arrays.stream(queryEnumClazz.getEnumConstants()).map(QueryEnum::getQueryClazz).collect(Collectors.toList());
        for (Class<? extends Query> queryEnumClazzVal : queryEnums) {
            run(joinOperator, queryEnumClazzVal);
        }
    }

    public static void main(String[] args)
    {
        runHelper(JoinOperator.HASH_JOIN, TPCHQueriesFindOptJoinTree.class);
        runHelper(JoinOperator.TTJHP, TPCHQueriesAutoGen.class);
        runHelper(JoinOperator.Yannakakis, TPCHQueriesYannakakis.class);
        runHelper(JoinOperator.YannakakisB, TPCHQueriesYannakakisB.class);
        runHelper(JoinOperator.HASH_JOIN, TPCHQueriesAutoGen.class);
        runHelper(JoinOperator.HASH_JOIN, TPCHQueriesHashJoinOnYannakakis.class);
    }
}
