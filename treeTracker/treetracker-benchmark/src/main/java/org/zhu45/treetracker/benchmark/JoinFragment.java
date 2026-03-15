package org.zhu45.treetracker.benchmark;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodList;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeIdAllocator;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;
import org.zhu45.treetracker.relational.planner.TestingCaseVerifier;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.Rule;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.common.Utils.appendCallerInfo;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.TableScanVisitor.gatherTableScanNodes;

public abstract class JoinFragment
        implements JoinFragmentType
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(JoinFragment.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected JdbcClient jdbcClient;
    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private Map<OptType, List<Class<? extends Operator>>> operatorMap = createMap(Optional.of(TupleBaseTreeTrackerOneBetaTableScanOperator.class),
            Optional.of(TupleBaseTreeTrackerOneBetaHashTableOperator.class));
    protected TestingCaseVerifier caseVerifier = new TestingCaseVerifier();
    protected List<Operator> operators;
    protected boolean stopAfterFullReducer;
    protected boolean disablePTOptimizationTrick;

    Plan physicalPlan;
    ExecutionNormal executionNormal;
    protected List<Rule> rules;
    protected NoGoodList noGoodList;

    protected void setOperatorMap(Map<OptType, List<Class<? extends Operator>>> operatorMap)
    {
        this.operatorMap = operatorMap;
    }

    protected void setPhysicalPlan(Plan physicalPlan)
    {
        this.physicalPlan = requireNonNull(physicalPlan, "Need to set physical plan first");
        this.executionNormal = new ExecutionNormal(physicalPlan.getRoot());
    }

    public void eval()
    {
        executionNormal.evalForBenchmark();
    }

    public void open()
    {
        executionNormal.open();
    }

    public long evalWithResultSize()
    {
        return executionNormal.evalForBenchmarkWithResultSize();
    }

    public void evalWithoutOpen()
    {
        executionNormal.evalForBenchmarkWithoutOpen();
    }

    public Pair<Plan, List<Operator>> createFixedPhysicalPlanFromQueryGraph(MultiwayJoinOrderedGraph multiwayJoinOrderedGraph)
    {
        requireNonNull(multiwayJoinOrderedGraph, "multiwayJoinOrderedGraph is null");
        requireNonNull(noGoodList, "noGoodList is null");

        PlanBuildContext context = builder()
                .setPlanNodeIdAllocator(idAllocator)
                .setRules(rules)
                .setOrderedGraph(multiwayJoinOrderedGraph)
                .setNoGoodList(noGoodList)
                .setOperatorMap(operatorMap)
                .disablePTOptimizationTrick(disablePTOptimizationTrick)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
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

    public Pair<Plan, List<Operator>> createPhysicalPlanFromPostgresPlan(String postgresPlanPath, List<SchemaTableName> schemaTableNameList)
    {
        try {
            PlanBuildContext.Builder builder = builder();
            PlanBuildContext context = builder
                    .setRules(rules)
                    .setPlanNodeIdAllocator(idAllocator)
                    .setOperatorMap(operatorMap)
                    .setJdbcClient(jdbcClient)
                    .postgresPlan(Files.readString(Paths.get(postgresPlanPath)))
                    .schemaTableNameList(schemaTableNameList)
                    .planBuildOption(PlanBuildContext.PlanBuildOption.POSTGRES)
                    .build();
            PlanBuilder planBuilder = new PlanBuilder(context);
            Plan logicalPlan = planBuilder.build();
            RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
            Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
            return Pair.of(physicalPlan, physicalPlan.getOperatorList());
        }
        catch (IOException e) {
            throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, "Postgres plan not found from: " + postgresPlanPath);
        }
    }

    public Plan createPhysicalPlanFromJoinOrdering(JoinOrdering joinOrdering, MultiwayJoinOrderedGraph joinTree)
    {
        PlanBuildContext.Builder builder = builder();
        builder.setRules(rules)
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .setJdbcClient(jdbcClient);
        if (joinTree != null) {
            builder.setOrderedGraph(joinTree);
        }
        PlanBuildContext context = builder.build();
        PlanBuilder planBuilder = new PlanBuilder(joinOrdering.getSchemaTableNameList(), context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        return physicalPlanBuilder.build(logicalPlan.getRoot());
    }

    public Plan createPhysicalPlanForYannakakis(SemiJoinOrdering semiJoinOrdering)
    {
        return createPhysicalPlanForYannakakis(semiJoinOrdering,
                TupleBasedLeftSemiHashJoinOperator.class,
                false,
                false,
                false);
    }

    public Plan createPhysicalPlanForYannakakis(SemiJoinOrdering semiJoinOrdering,
                                                Class<? extends TupleBasedJoinOperator> semiJoinClazz,
                                                boolean disablePTOptimizationTrick,
                                                boolean enableJoinGraphHeuristicFromPT,
                                                boolean skipTopDownSemijoins)
    {
        PlanBuildContext.Builder builder = builder();
        builder.setSemiJoinClazz(semiJoinClazz);
        builder.disablePTOptimizationTrick(disablePTOptimizationTrick);
        PlanBuildContext context = builder
                .setOrderedGraph(semiJoinOrdering.getJoinTree())
                .setSemijoinOrdering(semiJoinOrdering)
                .setRules(List.of(new AttachFullReducer()))
                .setPlanNodeIdAllocator(idAllocator)
                .setOperatorMap(operatorMap)
                .setJdbcClient(jdbcClient)
                .enableJoinGraphHeuristicFromPT(enableJoinGraphHeuristicFromPT)
                .skipTopDownSemijoins(skipTopDownSemijoins)
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        return physicalPlanBuilder.build(logicalPlan.getRoot());
    }

    public Operator getRootOperator()
    {
        return executionNormal.getRoot().getOperator();
    }

    public List<Operator> getOperators()
    {
        return operators;
    }

    public Plan getPlan()
    {
        return physicalPlan;
    }

    public void cleanUp()
    {
        getOperators().forEach(operator -> {
            if (operator.getMultiwayJoinNode() != null) {
                MultiwayJoinNode node = operator.getMultiwayJoinNode();
                node.getDomain().close();
            }
            operator.close();
        });
    }

    public void populateDomain()
    {
        checkState(physicalPlan != null);
        List<PlanNode> tableScanNodes = gatherTableScanNodes(physicalPlan.getRoot());
        List<Operator> tableScanOperators = tableScanNodes.stream().map(PlanNode::getOperator).collect(Collectors.toList());
        tableScanOperators.forEach(operator -> {
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug(appendCallerInfo("begin populating " + operator.getTraceOperatorName(), 2));
            }
            operator.setUseDomainAsSource(false);
            // For HJ, we assume there is a multiwayJoinNode binds to the operator even though HJ doesn't need it to work.
            MultiwayJoinDomain domain = operator.getMultiwayJoinNode().getDomain();
            domain.setUseDomainAsSource(false);
            domain.clear();
            operator.open();
            while (true) {
                Row row = operator.getNext();
                if (row == null) {
                    operator.close();
                    break;
                }
                domain.add(row);
            }
            operator.setUseDomainAsSource(true);
            domain.setUseDomainAsSource(true);
        });
    }
}
