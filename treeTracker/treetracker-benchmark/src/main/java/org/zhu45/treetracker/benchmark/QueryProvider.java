package org.zhu45.treetracker.benchmark;

import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformation;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformationContext;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformationFactory;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;
import org.zhu45.treetracker.relational.planner.rule.Rule;
import org.zhu45.treetracker.relational.statistics.CostModel;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.bloomSemiJoinContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.hashJoinContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.lipContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.pTOContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.semiJoinContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjHPBFContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjHPBGContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjHPContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjHPNONGContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjV1ContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.ttjV2ContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.yannakakisBContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.yannakakisContextBuilder;
import static org.zhu45.treetracker.benchmark.JoinFragmentContext.yannakakisVContextBuilder;
import static org.zhu45.treetracker.relational.operator.StatisticsInformationToJson.generateStatisticsInformationJson;

public class QueryProvider
{
    private QueryProvider()
    {
    }

    public static JoinFragmentType queryProvider(QueryProviderContext context)
    {
        try {
            Constructor<? extends Query> queryConstructor = context.queryClazz.getConstructor(JoinFragmentContext.class);
            switch (context.joinOperator) {
                case TTJHP:
                    JoinFragmentContext ttjHPContext = ttjHPContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(ttjHPContext);
                case TTJHP_NO_NG:
                    JoinFragmentContext ttjHPNONGContext = ttjHPNONGContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(ttjHPNONGContext);
                case TTJV2:
                    JoinFragmentContext ttjV2Context = ttjV2ContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(ttjV2Context);
                case TTJV1:
                    JoinFragmentContext ttjV1Context = ttjV1ContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(ttjV1Context);
                case TTJHP_BF:
                    JoinFragmentContext ttjHPBFContext = ttjHPBFContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(ttjHPBFContext);
                case TTJHP_BG:
                    JoinFragmentContext ttjHPBGContext = ttjHPBGContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(ttjHPBGContext);
                case HASH_JOIN:
                    JoinFragmentContext hashJoinContext = hashJoinContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(hashJoinContext);
                case LIP:
                    JoinFragmentContext lipContext = lipContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(lipContext);
                case Yannakakis:
                    List<Rule> newRules = new ArrayList<>();
                    if (context.rules != null) {
                        newRules.addAll(context.rules);
                    }
                    newRules.forEach(rule -> checkState(!(rule instanceof AttachFullReducer),
                            "AttachFullReducer rule is automatically added to Yannakakis's algorithm; no need to specify"));
                    newRules.add(new AttachFullReducer());
                    JoinFragmentContext yannakakisContext = yannakakisContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setStopAfterFullReducer(context.stopAfterFullReducer)
                            .setRules(newRules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(yannakakisContext);
                case YannakakisB:
                    List<Rule> newRulesB = new ArrayList<>();
                    if (context.rules != null) {
                        newRulesB.addAll(context.rules);
                    }
                    newRulesB.forEach(rule -> checkState(!(rule instanceof AttachFullReducer),
                            "AttachFullReducer rule is automatically added to Yannakakis's algorithm; no need to specify"));
                    newRulesB.add(new AttachFullReducer(TupleBasedLeftSemiBloomJoinOperator.class));
                    JoinFragmentContext yannakakisBContext = yannakakisBContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setStopAfterFullReducer(context.stopAfterFullReducer)
                            .setRules(newRulesB)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(yannakakisBContext);
                case YannakakisVanilla:
                    List<Rule> newRulesV = new ArrayList<>();
                    if (context.rules != null) {
                        newRulesV.addAll(context.rules);
                    }
                    newRulesV.forEach(rule -> checkState(!(rule instanceof AttachFullReducer),
                            "AttachFullReducer rule is automatically added to Yannakakis's algorithm; no need to specify"));
                    newRulesV.add(new AttachFullReducer());
                    JoinFragmentContext yannakakisVContext = yannakakisVContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setStopAfterFullReducer(context.stopAfterFullReducer)
                            .setRules(newRulesV)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(yannakakisVContext);
                case PTO:
                    List<Rule> newRulesPTO = new ArrayList<>();
                    if (context.rules != null) {
                        newRulesPTO.addAll(context.rules);
                    }
                    newRulesPTO.forEach(rule -> checkState(!(rule instanceof AttachFullReducer),
                            "AttachFullReducer rule is automatically added to Yannakakis's algorithm; no need to specify"));
                    // NOTE: we need to pass in enableJoinGraphHeuristicFromPT if we want to run BenchmarkSSB
                    // However, since PTO = YannakakisB in star-schema queries, we don't need to do so
                    newRulesPTO.add(new AttachFullReducer());
                    JoinFragmentContext pTOContext = pTOContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setStopAfterFullReducer(context.stopAfterFullReducer)
                            .setRules(newRulesPTO)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    return queryConstructor.newInstance(pTOContext);
                case TTJ:
                    JoinFragmentContext ttjContext = ttjContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    ttjContextBuilder.build();
                    return queryConstructor.newInstance(ttjContext);
                case SemiJoin:
                    JoinFragmentContext semiJoinContext = semiJoinContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    semiJoinContextBuilder.build();
                    return queryConstructor.newInstance(semiJoinContext);
                case BloomSemiJoin:
                    JoinFragmentContext bloomSemiJoinContext = bloomSemiJoinContextBuilder
                            .setBackJumpedRelationSize(context.backJumpedRelationSize)
                            .setNumberOfBackJumpedRelations(context.numberOfBackjumpedRelations)
                            .setQueryName(context.queryName)
                            .setRules(context.rules)
                            .setJdbcClient(context.jdbcClient)
                            .build();
                    bloomSemiJoinContextBuilder.build();
                    return queryConstructor.newInstance(bloomSemiJoinContext);
                default:
                    throw new UnsupportedOperationException(String.format("provided %s is not supported", context.joinOperator));
            }
        }
        catch (InvocationTargetException | InstantiationException | IllegalAccessException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static JoinFragmentType queryProvider(JoinOperator joinOperator, QueryEnum queryEnum, JdbcClient jdbcClient)
    {
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(joinOperator)
                .setQueryClazz(queryEnum.getQueryClazz())
                .setRules(List.of())
                .setJdbcClient(jdbcClient)
                .build();
        return queryProvider(context);
    }

    public static JoinFragmentType queryProvider(JoinOperator joinOperator, QueryEnum queryEnum, List<Rule> rules, JdbcClient jdbcClient)
    {
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(joinOperator)
                .setQueryClazz(queryEnum.getQueryClazz())
                .setRules(rules)
                .setJdbcClient(jdbcClient)
                .build();
        return queryProvider(context);
    }

    public static void benchmarkQuery(JoinOperator joinOperator, Class<? extends Query> queryClazz)
    {
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(joinOperator)
                .setQueryClazz(queryClazz)
                .build();
        JoinFragmentType query = queryProvider(context);
        try {
            query.eval();
        }
        finally {
            query.cleanUp();
        }
    }

    public static AggregateStatisticsInformation gatherQueryStatistics(JoinOperator joinOperator, Class<? extends Query> queryClazz, Optional<String> queryName)
    {
        QueryProviderContext.Builder contextBuilder = QueryProviderContext.builder()
                .setJoinOperator(joinOperator)
                .setQueryClazz(queryClazz);
        queryName.ifPresentOrElse(contextBuilder::setQueryName, null);
        QueryProviderContext queryProviderContext = contextBuilder.build();
        JoinFragmentType query = queryProvider(queryProviderContext);
        try {
            Runtime rt = Runtime.getRuntime();
            long totalMemory = rt.totalMemory();
            long evaluationTimeMarker = System.nanoTime();
            long resultSize = query.evalWithResultSize();
            long runtime = System.nanoTime() - evaluationTimeMarker;
            long freeMemory = rt.freeMemory();
            Operator rootOperator = query.getRootOperator();
            AggregateStatisticsInformationContext context = AggregateStatisticsInformationContext.builder()
                    .setRootOperator(rootOperator)
                    .setResultSetSize(resultSize)
                    .setJoinOperator(joinOperator)
                    .setQueryName(queryClazz.getCanonicalName())
                    .setNumRelations(query.getNumRelations())
                    .setRuntime(TimeUnit.NANOSECONDS.toMillis(runtime))
                    .setEvaluationMemoryCostInBytes(totalMemory - freeMemory)
                    .build();
            AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(context);
            return factory.get();
        }
        finally {
            query.cleanUp();
        }
    }

    public static void createStatisticsJson(JoinFragmentType query, String resultStoredPath)
    {
        Runtime rt = Runtime.getRuntime();
        long evaluationTimeMarker = System.nanoTime();
        long resultSize = query.evalWithResultSize();
        long runtime = System.nanoTime() - evaluationTimeMarker;
        long totalMemory = rt.totalMemory();
        long freeMemory = rt.freeMemory();
        checkState(totalMemory > freeMemory,
                String.format("totalMemory: %s < freeMemory: %s", totalMemory, freeMemory));
        Operator rootOperator = query.getRootOperator();
        AggregateStatisticsInformationContext context = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setResultSetSize(resultSize)
                .setJoinOperator(query.getAlgorithm())
                .setQueryName(query.getQueryName())
                .setNumRelations(query.getNumRelations())
                .setRuntime(TimeUnit.NANOSECONDS.toMillis(runtime))
                .setEvaluationMemoryCostInBytes(totalMemory - freeMemory)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(context);
        AggregateStatisticsInformation aggregateStatisticsInformation = factory.get();
        generateStatisticsInformationJson(rootOperator, aggregateStatisticsInformation, query.getAlgorithm(), query.getQueryName(), resultStoredPath);
    }

    public static void createCostStatisticsJson(JoinFragmentType query, CostModel costModel, String resultStoredPath)
    {
        Operator rootOperator = query.getRootOperator();
        AggregateStatisticsInformationContext context = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(query.getAlgorithm())
                .setQueryName(query.getQueryName())
                .setNumRelations(query.getNumRelations())
                .setCostModel(costModel)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(context);
        AggregateStatisticsInformation aggregateStatisticsInformation = factory.get();
        generateStatisticsInformationJson(null, aggregateStatisticsInformation, query.getAlgorithm(), query.getQueryName(), resultStoredPath);
    }
}
