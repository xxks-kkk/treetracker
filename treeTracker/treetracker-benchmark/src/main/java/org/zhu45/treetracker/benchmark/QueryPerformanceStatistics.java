package org.zhu45.treetracker.benchmark;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.ssb.SSBQueries;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_SQLITE_ORDERING_STATS_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class QueryPerformanceStatistics
{
    private QueryPerformanceStatistics()
    {
    }

    public static void main(String[] args)
    {
        long timeNow = System.currentTimeMillis();
        SSBQueries queries = SSBQueries.Q3P4;
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queries, duckDBJdbcClientSupplier.get());
        StatisticsInformationPrinter printer = new StatisticsInformationPrinter();
        PlanPrinter planPrinter = new PlanPrinter(query.getPlan().getRoot());
        try {
            createStatisticsJson(query, SSB_SQLITE_ORDERING_STATS_STORED_PATH);
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
            System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            System.out.println(planPrinter.toText(0));
            System.out.println(printer.print(query.getRootOperator()));
        }
    }
}
