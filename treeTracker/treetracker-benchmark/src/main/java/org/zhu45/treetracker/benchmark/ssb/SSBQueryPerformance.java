package org.zhu45.treetracker.benchmark.ssb;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.FullReducerOperator;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstYannakakisProvider;
import org.zhu45.treetracker.relational.planner.cost.TTJCardEstProvider;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.Rule;

import java.util.List;

import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.createStatisticsJson;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration.defaultConfiguration;

public class SSBQueryPerformance
{
    private static final LoggerProvider.TreeTrackerLogger log;

    static {
        if (Switches.DEBUG) {
            log = getLogger(SSBQueryPerformance.class);
        }
        else {
            log = null;
        }
    }

    private SSBQueryPerformance()
    {
    }

    private static void run(JoinOperator algorithm, Class<? extends Query> queryClazz, boolean enableRules)
    {
        System.out.println("JoinOperator: " + algorithm);
        System.out.println("queryClazz: " + queryClazz);
        if (Switches.DEBUG && checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(FindTheBestJoinOrderingJoinTreeWithDP.class.getName(), Level.TRACE);
            Configurator.setAllLevels(JoinTreeCostEstProvider.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TTJCardEstProvider.class.getName(), Level.TRACE);
            Configurator.setAllLevels(FindTheBestJoinOrderingWithDP.class.getName(), Level.TRACE);
            Configurator.setAllLevels(JoinOrdering.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(FullReducerOperator.class.getName(), Level.DEBUG);
        }
        StatisticsInformationPrinter printer;
        if (Switches.STATS) {
            printer = new StatisticsInformationPrinter();
        }
        List<Rule> rules;
        if (algorithm == JoinOperator.TTJHP && enableRules) {
            rules = List.of(new FindTheBestJoinOrderingJoinTreeWithDP(new TTJCardEstProvider(new JoinTreeCostEstProvider(defaultConfiguration))));
        }
        else if (algorithm == JoinOperator.Yannakakis && enableRules) {
            rules = List.of(new FindTheBestJoinOrderingJoinTreeWithDP(new TTJCardEstProvider(new JoinTreeCostEstYannakakisProvider(JoinTreeCostEstYannakakisProvider.defaultConfiguration))));
        }
        else {
            rules = List.of();
        }
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(algorithm)
                .setQueryClazz(queryClazz)
                .setRules(rules)
                .setJdbcClient(duckDBJdbcClientSupplier.get())
                .build();
        JoinFragmentType query = queryProvider(context);
        long timeNow = System.currentTimeMillis();
        if (Switches.DEBUG) {
            System.out.println("Starting timing");
        }
        try {
            createStatisticsJson(query, SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH);
        }
        finally {
            if (Switches.DEBUG) {
                System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            }
            if (Switches.STATS) {
                System.out.println(printer.print(query.getRootOperator()));
            }
            if (enableRules) {
                query.getPlan().getPlanStatistics().save(query.getQueryName(),
                        query.getAlgorithm(), SSB_RESULT_STORED_PATH);
            }
        }
    }

    public static void main(String[] args)
    {
        run(JoinOperator.Yannakakis1Pass, Query2P1.class, false);
    }
}
