package org.zhu45.treetracker.benchmark.tupleFetch;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;

import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class TupleFetchingPerformance
{
    private TupleFetchingPerformance() {}

    public static void main(String[] args)
    {
        long timeNow = System.currentTimeMillis();
        TupleFetchQueries queries = TupleFetchQueries.TupleFetchQuery;
        Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.WARN);
        JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN, queries, duckDBJdbcClientSupplier.get());
        StatisticsInformationPrinter printer = new StatisticsInformationPrinter();
        query.populateDomain();
        try {
            createStatisticsJson(query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
            System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            System.out.println(printer.print(query.getRootOperator()));
        }
    }
}
