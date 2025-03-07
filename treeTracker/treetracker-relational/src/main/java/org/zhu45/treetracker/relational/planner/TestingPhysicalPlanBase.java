package org.zhu45.treetracker.relational.planner;

import lombok.Getter;
import lombok.NonNull;
import org.apache.commons.collections4.MultiSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.OrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.PostgresNaturalJoinExecutor;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.jdbc.testing.Database;
import org.zhu45.treetracker.jdbc.testing.NaturalJoinJdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionBase;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodList;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.Rule;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import javax.annotation.Nullable;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.common.Utils.properPrintList;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.columnCompare;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.rowCompare;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.defaultOperatorMap;

/**
 * Create physical plans for various tests
 */
public class TestingPhysicalPlanBase<T extends ExecutionBase>
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(TestingPhysicalPlanBase.class);
    private Logger traceLogger = LogManager.getLogger(TestingPhysicalPlanBase.class.getName());

    @Getter
    private PlanNodeIdAllocator idAllocator;
    private JdbcClient jdbcClient;
    private Database database;
    private ConnectionLeakUtil connectionLeakUtil;
    private Optional<Long> seed;
    private Random rand;
    private PostgresNaturalJoinExecutor postgresNaturalJoinExecutor;
    private Class<T> executionClazz;
    @Getter
    private Map<OptType, List<Class<? extends Operator>>> operatorMap;
    private Integer testCaseNumber = 0;
    private TestingCaseVerifier caseVerifier;
    private List<Rule> rules;

    public TestingPhysicalPlanBase(@NonNull Database database,
                                   String naturalJoinTable,
                                   @NonNull List<Rule> rules,
                                   @Nullable Class<T> executionClazz,
                                   @NonNull Optional<Long> seed,
                                   @NonNull Optional<Map<OptType, List<Class<? extends Operator>>>> operatorMap)
    {
        this.database = database;
        this.idAllocator = new PlanNodeIdAllocator();
        this.jdbcClient = database.getJdbcClient();
        this.connectionLeakUtil = new ConnectionLeakUtil();
        this.seed = seed;
        this.rand = new Random();
        seed.ifPresent(num -> rand.setSeed(num));
        this.postgresNaturalJoinExecutor = new PostgresNaturalJoinExecutor(database, naturalJoinTable);
        this.caseVerifier = new TestingCaseVerifier();
        this.rules = rules;
        this.executionClazz = executionClazz;
        operatorMap.ifPresentOrElse(
                map -> this.operatorMap = map,
                () -> this.operatorMap = defaultOperatorMap);
    }

    public void updateRules(List<Rule> rules)
    {
        this.rules = rules;
    }

    public Database getDatabase()
    {
        return database;
    }

    /**
     * @param expectedSchemaTableNames the expected list of schemas (used to verify the generated plan is exactly what is expected)
     */
    public Pair<Plan, List<Operator>> createFixedPhysicalPlanFromQueryGraph(MultiwayJoinOrderedGraph multiwayJoinOrderedGraph,
                                                                            Optional<LinkedList<SchemaTableName>> expectedSchemaTableNames,
                                                                            Optional<NoGoodList> noGoodList)
    {
        requireNonNull(multiwayJoinOrderedGraph, "multiwayJoinOrderedGraph is null");
        requireNonNull(expectedSchemaTableNames, "expectedSchemaTableNames is null");

        PlanBuildContext.Builder builder = builder();
        noGoodList.ifPresent(builder::setNoGoodList);
        PlanBuildContext context = builder
                .setOrderedGraph(multiwayJoinOrderedGraph)
                .setRules(rules)
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        expectedSchemaTableNames.ifPresent(schemaTableNames -> assertTrue(caseVerifier.visitPlan(logicalPlan.getRoot(), schemaTableNames)));
        context.setJdbcClient(jdbcClient);
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
        return Pair.of(physicalPlan, physicalPlan.getOperatorList());
    }

    public Pair<Plan, List<Operator>> createFixedPhysicalPlanFromQueryGraph(PlanBuildContext context,
                                                                            Optional<LinkedList<SchemaTableName>> expectedSchemaTableNames)
    {
        requireNonNull(expectedSchemaTableNames, "expectedSchemaTableNames is null");
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        expectedSchemaTableNames.ifPresent(schemaTableNames -> assertTrue(caseVerifier.visitPlan(logicalPlan.getRoot(), schemaTableNames)));
        context.setJdbcClient(jdbcClient);
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
        return Pair.of(physicalPlan, physicalPlan.getOperatorList());
    }

    public Pair<Plan, List<Operator>> createPhysicalPlanFromJoinOrdering(JoinOrdering joinOrdering)
    {
        PlanBuildContext.Builder builder = builder();
        PlanBuildContext context = builder
                .setRules(rules)
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .setJdbcClient(jdbcClient)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(joinOrdering.getSchemaTableNameList(), context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
        return Pair.of(physicalPlan, physicalPlan.getOperatorList());
    }

    public Pair<Plan, List<Operator>> createPhysicalPlanForYannakakis(SemiJoinOrdering semiJoinOrdering)
    {
        PlanBuildContext.Builder builder = builder();
        PlanBuildContext context = builder
                .setOrderedGraph(semiJoinOrdering.getJoinTree())
                .setSemijoinOrdering(semiJoinOrdering)
                .setRules(List.of(new AttachFullReducer()))
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .setJdbcClient(jdbcClient)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
        return Pair.of(physicalPlan, physicalPlan.getOperatorList());
    }

    public Pair<Plan, List<Operator>> createPhysicalPlanFromPostgresPlan(String postgresPlan, String schema)
    {
        PlanBuildContext.Builder builder = builder();
        PlanBuildContext context = builder
                .setRules(rules)
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .setJdbcClient(jdbcClient)
                .postgresPlan(postgresPlan)
                .schema(schema)
                .planBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
        return Pair.of(physicalPlan, physicalPlan.getOperatorList());
    }

    public Pair<Plan, List<Operator>> createPhysicalPlanFromPostgresPlan(String postgresPlan, List<SchemaTableName> schemaTableNames)
    {
        PlanBuildContext.Builder builder = builder();
        PlanBuildContext context = builder
                .setRules(List.of())
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .setJdbcClient(jdbcClient)
                .postgresPlan(postgresPlan)
                .schemaTableNameList(schemaTableNames)
                .planBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
        return Pair.of(physicalPlan, physicalPlan.getOperatorList());
    }

    public void testPhysicalPlanExecution(Pair<Plan, List<Operator>> pair)
    {
        requireNonNull(executionClazz, "executionClazz is null");
        testCaseNumber++;
        log.info("test case number: " + testCaseNumber);
        traceLogger.trace("\n\ntest case number: " + testCaseNumber);
        List<SchemaTableName> schemaTableNames = getSchemaTableNames(pair.getKey().getRoot());
        PlanPrinter printer = new PlanPrinter(pair.getKey().getRoot());
        log.debug(printer.toText(0));
        OrderedGraph orderedGraph = pair.getKey().getRoot().getOperator().getPlanBuildContext().getOrderedGraph();
        if (orderedGraph != null) {
            log.debug("query graph:\n" + orderedGraph);
        }
        MultiSet<Row> expected = postgresNaturalJoinExecutor.executeNaturalJoinOnPostgres(schemaTableNames);
        MultiSet<Row> actual = null;
        try {
            Constructor<T> constructor = executionClazz.getConstructor(PlanNode.class);
            T execution = (T) constructor.newInstance(pair.getKey().getRoot());
            actual = execution.eval();
            log.infoBlueBoldBright("expected: \n" + properPrintList(expected));
            log.infoBlueBoldBright("actual: \n" + properPrintList(actual));
            boolean rowCompareRes = rowCompare(expected, actual);
            boolean columnCompareRes = columnCompare(expected, actual);
            log.debug("rowCompareRes: " + rowCompareRes);
            log.debug("columnCompareRes: " + columnCompareRes);
            assertTrue(rowCompareRes && columnCompareRes);
        }
        catch (AssertionError e) {
            StringBuilder builder = new StringBuilder();
            builder.append("testPhysicalPlanExecution").append("\n");
            builder.append(String.format("seed for %s database: \n%s\n", database.getClass().getName(), database.getSeed()));
            builder.append(String.format("seed for plan: \n%s\n", seed));
            builder.append("physical plan: \n");
            builder.append(printer.toText(0));
            if (orderedGraph != null) {
                builder.append("query graph: ").append("\n");
                builder.append(orderedGraph).append("\n");
            }
            for (SchemaTableName schemaTableName : schemaTableNames) {
                RowSet rowSet = database.getRelationRows(schemaTableName.getTableName());
                builder.append("Table ").append(schemaTableName).append("\n");
                builder.append(rowSet.renderOutput()).append("\n");
            }
            builder.append("postgres SQL: \n").append(((NaturalJoinJdbcClient) database.getJdbcClient()).getNaturalJoinSql()).append("\n");
            builder.append("expected: \n").append(new RowSet(new ArrayList<>(expected)).renderOutput()).append("\n");
            builder.append("actual: \n").append(new RowSet(new ArrayList<>(actual)).renderOutput()).append("\n");
            Operator rootOperator = pair.getKey().getRoot().getOperator();
            StatisticsInformationPrinter statsPrinter = new StatisticsInformationPrinter();
            String statistics = statsPrinter.print(rootOperator);
            builder.append("statistics: \n")
                    .append(statistics);
            builder.append("stacktrace: \n")
                    .append(e.getLocalizedMessage()).append("\n")
                    .append(properPrintList(Arrays.asList(e.getStackTrace()))).append("\n");
            log.error(builder.toString());
            fail();
        }
        catch (Exception e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
        finally {
            pair.getValue().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
        }
    }

    public void tearDown()
            throws Exception
    {
        database.getJdbcClient().getConnection().close();
        connectionLeakUtil.assertNoLeaks();
    }

    public TestingCaseVerifier getCaseVerifier()
    {
        return this.caseVerifier;
    }

    public static void cleanUp(List<Operator> operators)
    {
        operators.forEach(operator -> {
            if (operator.getMultiwayJoinNode() != null) {
                MultiwayJoinNode node = operator.getMultiwayJoinNode();
                ((MultiwayJoinDomain) node.getDomain()).close();
            }
            operator.close();
        });
    }
}
