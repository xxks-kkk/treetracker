package org.zhu45.treetracker.benchmark.tpch;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.TTJCardEstProvider;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;

import java.util.List;

import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration.defaultConfiguration;

public class TPCHQueryPerformance
{
    private static final LoggerProvider.TreeTrackerLogger log;

    static {
        if (Switches.DEBUG) {
            log = getLogger(TPCHQueryPerformance.class);
        }
        else {
            log = null;
        }
    }

    private TPCHQueryPerformance()
    {
    }

    private static void run(JoinOperator algorithm, Class<? extends Query> queryClazz)
    {
        System.out.println("JoinOperator: " + algorithm);
        System.out.println("queryClazz: " + queryClazz);
        if (Switches.DEBUG && checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(FindTheBestJoinOrderingJoinTreeWithDP.class.getName(), Level.TRACE);
        }
        StatisticsInformationPrinter printer;
        if (Switches.STATS) {
            printer = new StatisticsInformationPrinter();
        }
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(algorithm)
                .setQueryClazz(queryClazz)
                .setRules(List.of(new FindTheBestJoinOrderingJoinTreeWithDP(new TTJCardEstProvider(new JoinTreeCostEstProvider(defaultConfiguration)),
                        FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration.builder().enableHJPhysicalHeuristics(true).build())))
                .setJdbcClient(duckDBJdbcClientSupplier.get())
                .build();
        JoinFragmentType query = queryProvider(context);
        long timeNow = System.currentTimeMillis();
        if (Switches.DEBUG) {
            System.out.println("Starting timing");
        }
        try {
            if (Switches.STATS) {
                createStatisticsJson(query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
            }
            else {
                query.eval();
            }
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
            if (Switches.DEBUG) {
                System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            }
            if (Switches.STATS) {
                System.out.println(printer.print(query.getRootOperator()));
                query.getPlan().getPlanStatistics().save(query.getQueryName(),
                        query.getAlgorithm(), TPCH_WITH_PREDICATES_RESULT_STORED_PATH);
            }
        }
    }

    public static void main(String[] args)
    {
        run(JoinOperator.TTJHP, Query3WOptJoinTreeOptOrdering.class);
    }
}
