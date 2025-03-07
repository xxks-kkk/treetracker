package org.zhu45.treetracker.benchmark.micro;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.jdbc.ObjectRecordTupleSource;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.FullReducerOperator;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;

import java.util.List;

import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class MicroBenchQueryPerformance
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(MicroBenchQueryPerformance.class);

    private MicroBenchQueryPerformance() {}

    public static void main(String[] args)
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(ObjectRecordTupleSource.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(FullReducerOperator.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(ExecutionNormal.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
        }
        StatisticsInformationPrinter printer;
        if (Switches.DEBUG) {
            printer = new StatisticsInformationPrinter();
        }
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(JoinOperator.HASH_JOIN)
                .setQueryClazz(TwoWayJoinQuery1.class)
                .setRules(List.of())
                .setJdbcClient(duckDBJdbcClientSupplier.get())
                .build();
        JoinFragmentType query = queryProvider(context);
        long timeNow = System.currentTimeMillis();
        if (Switches.DEBUG) {
            System.out.println("Starting timing");
        }
        try {
            query.eval();
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
            if (Switches.DEBUG) {
                System.out.println(printer.print(query.getRootOperator()));
            }
            query.getPlan().getPlanStatistics().save(query.getQueryName(),
                    query.getAlgorithm(), JOB_WITH_PREDICATES_RESULT_STORED_PATH);
        }
    }
}
