package org.zhu45.treetracker.benchmark.job;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesFindOptJoinTree;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstProvider;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostEstYannakakisProvider;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostProviderConfiguration;
import org.zhu45.treetracker.relational.planner.cost.PostgresCardEstProvider;
import org.zhu45.treetracker.relational.planner.cost.TTJCardEstProvider;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingJoinTreeWithDP;
import org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_TRUE_CARD_STORED_PATH;
import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.duckDBJdbcClientSupplier;

public class CheckDPMatrix
{
    private CheckDPMatrix()
    {
    }

    private static void checkDPMatrix(QueryEnum queryEnum,
                                      JoinOperator joinOperator,
                                      JoinTreeCostProviderConfiguration config,
                                      FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration)
    {
        System.out.println("QueryEnum: " + queryEnum);
        System.out.println("joinOperator: " + joinOperator);
        long timeStart = System.nanoTime();
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(FindTheBestJoinOrderingJoinTreeWithDP.class.getName(), Level.TRACE);
        }
        if (joinOperator == JoinOperator.HASH_JOIN) {
            checkDPMatirxHJ(queryEnum, config.isUseTrueCard(), ruleConfiguration);
        }
        else if (joinOperator == JoinOperator.TTJHP) {
            checkDPMatirxTTJ(queryEnum, joinOperator, config, ruleConfiguration);
        }
        else if (joinOperator == JoinOperator.Yannakakis) {
            checkDPMatirxYannakakis(queryEnum, joinOperator, config, ruleConfiguration);
        }
        System.out.println("time take: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - timeStart) + " ms");
    }

    private static void checkDPMatirxTTJ(QueryEnum queryEnum,
                                         JoinOperator joinOperator,
                                         JoinTreeCostProviderConfiguration config,
                                         FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration)
    {
        checkArgument(joinOperator == JoinOperator.TTJHP || joinOperator == JoinOperator.TTJHP_NO_NG);
        JoinFragmentType query = queryProvider(joinOperator,
                queryEnum,
                List.of(new FindTheBestJoinOrderingJoinTreeWithDP(
                        new TTJCardEstProvider(
                                new JoinTreeCostEstProvider(config)),
                        ruleConfiguration)),
                duckDBJdbcClientSupplier.get());
        FindTheBestJoinOrderingWithDP.DPTable dpTable = query.getPlan().getPlanStatistics().getDpTable();
        query.getPlan().getPlanStatistics().save(query.getQueryName(),
                query.getAlgorithm(), getSavePath(config.isUseTrueCard()));
        System.out.println(dpTable);
        saveDPMatrixToFile(dpTable, queryEnum, joinOperator, config.isUseTrueCard(), config);
    }

    private static void checkDPMatirxYannakakis(QueryEnum queryEnum,
                                         JoinOperator joinOperator,
                                         JoinTreeCostProviderConfiguration config,
                                         FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration)
    {
        checkArgument(joinOperator == JoinOperator.Yannakakis);
        JoinFragmentType query = queryProvider(joinOperator,
                queryEnum,
                List.of(new FindTheBestJoinOrderingJoinTreeWithDP(new TTJCardEstProvider(new JoinTreeCostEstYannakakisProvider(config)),
                        ruleConfiguration)),
                duckDBJdbcClientSupplier.get());
        FindTheBestJoinOrderingWithDP.DPTable dpTable = query.getPlan().getPlanStatistics().getDpTable();
        query.getPlan().getPlanStatistics().save(query.getQueryName(),
                query.getAlgorithm(), getSavePath(config.isUseTrueCard()));
        System.out.println(dpTable);
        saveDPMatrixToFile(dpTable, queryEnum, joinOperator, config.isUseTrueCard(), config);
    }

    private static void checkDPMatirxHJ(QueryEnum queryEnum, boolean useTrueCard, FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration)
    {
        JoinFragmentType query = queryProvider(JoinOperator.HASH_JOIN,
                queryEnum,
                List.of(new FindTheBestJoinOrderingWithDP(new PostgresCardEstProvider(useTrueCard), ruleConfiguration)),
                duckDBJdbcClientSupplier.get());
        FindTheBestJoinOrderingWithDP.DPTable dpTable = query.getPlan().getPlanStatistics().getDpTable();
        query.getPlan().getPlanStatistics().save(query.getQueryName(), query.getAlgorithm(), getSavePath(useTrueCard));
        System.out.println(dpTable);
        saveDPMatrixToFile(dpTable, queryEnum, JoinOperator.HASH_JOIN, useTrueCard, null);
    }

    private static String getSavePath(boolean useTrueCard)
    {
        if (useTrueCard) {
            return JOB_WITH_PREDICATES_RESULT_TRUE_CARD_STORED_PATH;
        }
        else {
            return JOB_WITH_PREDICATES_RESULT_STORED_PATH;
        }
    }

    private static void saveDPMatrixToFile(FindTheBestJoinOrderingWithDP.DPTable dpTable,
                                           QueryEnum queryEnum,
                                           JoinOperator joinOperator,
                                           boolean useTrueCard,
                                           JoinTreeCostProviderConfiguration configuration)
    {
        String path = Paths.get(getSavePath(useTrueCard), queryEnum + "_" + joinOperator + "_" + useTrueCard + "_" + "dp_matrix.txt").toString();
        try (PrintWriter out = new PrintWriter(path)) {
            out.println(dpTable.toString());
            if (configuration != null) {
                out.println(configuration);
            }
            System.out.println("dp matrix saved to: " + path);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
    {
        FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration ruleConfiguration =
                FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration.builder()
                        .enableTTJExecutionForEachCell(false)
                        .enableHJPhysicalHeuristics(true)
                        .build();
        JoinTreeCostProviderConfiguration config = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
                .useTrueCard(false)
                .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
                .includeInnerRelationSize(true)
                .build();
        checkDPMatrix(TPCHQueriesFindOptJoinTree.Query7aWFindOptJoinTree, JoinOperator.HASH_JOIN, config, ruleConfiguration);
    }
}
