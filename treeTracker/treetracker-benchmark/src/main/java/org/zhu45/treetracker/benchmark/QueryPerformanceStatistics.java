package org.zhu45.treetracker.benchmark;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.RedissonClientSupplier.redissonClientSupplier;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class QueryPerformanceStatistics
{
    private QueryPerformanceStatistics()
    {
    }

    public static void main(String[] args)
    {
        long timeNow = System.currentTimeMillis();
        JOBQueries queries = JOBQueries.Q1cOptJoinTreeOptOrdering;
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries, duckDBJdbcClientSupplier.get());
        StatisticsInformationPrinter printer = new StatisticsInformationPrinter();
        PlanPrinter planPrinter = new PlanPrinter(query.getPlan().getRoot());
        try {
            createStatisticsJson(query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
            if (!redissonClientSupplier.get().isShutdown()) {
                redissonClientSupplier.get().shutdown();
            }
            System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            System.out.println(planPrinter.toText(0));
            System.out.println(printer.print(query.getRootOperator()));
        }
    }
}
