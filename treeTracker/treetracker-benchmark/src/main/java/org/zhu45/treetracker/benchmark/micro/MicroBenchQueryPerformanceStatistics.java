package org.zhu45.treetracker.benchmark.micro;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;

import static org.zhu45.treetracker.benchmark.Benchmarks.MICROBENCH_QUERIES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.RedissonClientSupplier.redissonClientSupplier;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class MicroBenchQueryPerformanceStatistics
{
    private MicroBenchQueryPerformanceStatistics() {}

    public static void main(String[] args)
    {
        long timeNow = System.currentTimeMillis();
        MicroBenchQueries queries = MicroBenchQueries.TwoWayJoinQuery1;
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP_NO_NG, queries, duckDBJdbcClientSupplier.get());
        StatisticsInformationPrinter printer = new StatisticsInformationPrinter();
        try {
            createStatisticsJson(query, MICROBENCH_QUERIES_RESULT_STORED_PATH);
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
            if (!redissonClientSupplier.get().isShutdown()) {
                redissonClientSupplier.get().shutdown();
            }
            System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            System.out.println(printer.print(query.getRootOperator()));
        }
    }
}
