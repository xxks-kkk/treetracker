package org.zhu45.treetracker.relational.operator;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.ColumnHandle;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.inject.internal.util.Preconditions.checkArgument;
import static com.google.inject.internal.util.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.common.Utils.appendCallerInfo;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.gatherOuterTables;

public class FullReducerOperator
        extends AbstractOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(FullReducerOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private Operator sinkOperator;
    private static final OptType operatorType = OptType.fullReducer;
    private boolean canBeEarlyStopped;
    private boolean skipTopDownSemijoins;

    private List<Plan> bottomUpSemijoins;
    private List<Plan> topDownSemijoins;

    // key: relation Value: <R_n, R_n^*>, i.e., initial relation size and fully reduced relation size
    private Map<MultiwayJoinNode, List<Integer>> costMap;
    private long fullReducerTimerMarker;
    private long fullReducerTime;
    private Map<Integer, List<Operator>> node2Operators;

    public FullReducerOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new FullReducerStatisticsInformation();
            costMap = new HashMap<>();
        }
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitFullReducerOperator(this, context);
    }

    @Override
    public void open()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("enter open()"));
            incrementTraceDepth();
        }
        if (Switches.STATS) {
            fullReducerTimerMarker = System.nanoTime();
        }
        try {
            if (Switches.STATS) {
                for (List<Operator> operators : node2Operators.values()) {
                    costMap.computeIfAbsent(operators.get(0).getMultiwayJoinNode(),
                            k -> new ArrayList<>(List.of((int) getDomainOriginalSize(operators.get(0)), 0)));
                }
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("about to execute bottom-up semijoins ..."));
            }
            // We first perform semijoin reduction bottom-up: from leaf to the root, i.e., parent \leftsemijoin child
            boolean canBeEarlyStoppedFromBottomUp = executeSemiJoins(bottomUpSemijoins);
            if (canBeEarlyStoppedFromBottomUp) {
                canBeEarlyStopped = true;
                cleanUp();
                if (Switches.STATS) {
                    ((FullReducerStatisticsInformation) statisticsInformation).setEarlyStoppedDueToBottomUpPass(true);
                    fullReducerTime = System.nanoTime() - fullReducerTimerMarker;
                    ((FullReducerStatisticsInformation) statisticsInformation).setFullReducerTime(fullReducerTime);
                }
                return;
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("about to execute top-down semijoins ..."));
            }
            if (!skipTopDownSemijoins) {
                // We then perform semijoin reduction top-down: from root to the leaves, i.e., child \leftsemijoin parent
                boolean canBeEarlyStoppedFromTopDown = executeSemiJoins(topDownSemijoins);
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    traceLogger.debug(appendCallerInfo("finish the execution of full reducer", 2));
                }
                if (canBeEarlyStoppedFromTopDown) {
                    canBeEarlyStopped = true;
                    cleanUp();
                    if (Switches.STATS) {
                        ((FullReducerStatisticsInformation) statisticsInformation).setEarlyStoppedDueToTopDownPass(true);
                        fullReducerTime = System.nanoTime() - fullReducerTimerMarker;
                        ((FullReducerStatisticsInformation) statisticsInformation).setFullReducerTime(fullReducerTime);
                    }
                    return;
                }
            }
            if (Switches.STATS) {
                ((FullReducerStatisticsInformation) statisticsInformation).setCostMap(costMap);
                fullReducerTime = System.nanoTime() - fullReducerTimerMarker;
                ((FullReducerStatisticsInformation) statisticsInformation).setFullReducerTime(fullReducerTime);
            }
            sinkOperator.open();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                decrementTraceDepth();
            }
        }
        catch (TreeTrackerException e) {
            close();
            throw e;
        }
    }

    /**
     * Return true if some semijoin lead to one of the relations become empty,
     * which indicates Yannakakis can be early stopped.
     */
    private boolean executeSemiJoins(List<Plan> semijoins)
    {
        for (Plan plan : semijoins) {
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                PlanPrinter printer = new PlanPrinter(plan.getRoot());
                traceLogger.debug(printer.toText(0));
            }
            List<TableNode> outerTables = new ArrayList<>();
            gatherOuterTables(plan.getRoot(), outerTables);
            checkState(outerTables.size() == 1, "We expect there should be 1 table with Side.OUTER but got " + outerTables.size());
            Operator multiwayJoinParentOperator = outerTables.get(0).getOperator();
            MultiwayJoinNode multiwayJoinParentNode = multiwayJoinParentOperator.getMultiwayJoinNode();
            MultiwayJoinDomain multiwayJoinParentDomain = multiwayJoinParentNode.getDomain();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                String multiwayJoinParentName = multiwayJoinParentNode.getNodeName();
                PlanPrinter printer = new PlanPrinter(plan.getRoot());
                traceLogger.trace(formatTraceMessage(printer.toText(0)));
                traceLogger.trace(formatTraceMessage(String.format("%s domain (before semijoin): %s",
                        multiwayJoinParentName,
                        multiwayJoinParentDomain)));
            }
            ExecutionNormal executionNormal = new ExecutionNormal(plan.getRoot());
            RowSet multiwayJoinParentReducedDomainAsRowSet = new RowSet(executionNormal.eval());
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("%s original size: %s",
                        multiwayJoinParentNode.getNodeName(), getDomainOriginalSize(multiwayJoinParentOperator))));
                traceLogger.trace(formatTraceMessage(String.format("after reduced size: %s",
                        multiwayJoinParentReducedDomainAsRowSet.size())));
                traceLogger.trace(formatTraceMessage(String.format("tuples removed: %s",
                        getDomainOriginalSize(multiwayJoinParentOperator) - multiwayJoinParentReducedDomainAsRowSet.size())));
            }
            if (Switches.STATS) {
                String multiwayJoinParentName = multiwayJoinParentNode.getNodeName();
                AggregateStatisticsInformation aggregateStatisticsInformation = new AggregateStatisticsInformation();
                aggregateStatisticsInformation.printHelper(plan.getRoot().getOperator());
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("numberOfR1Assignments: " + aggregateStatisticsInformation.getNumberOfR1Assignments() + multiwayJoinParentReducedDomainAsRowSet.size() + 1));
                    traceLogger.trace(formatTraceMessage("multiwayJoinParentReducedDomainAsRowSet: " + multiwayJoinParentReducedDomainAsRowSet.size()));
                }
                // +1 for the null return
                statisticsInformation.updateNumberOfR1Assignments(aggregateStatisticsInformation.getNumberOfR1Assignments() + multiwayJoinParentReducedDomainAsRowSet.size() + 1);
                statisticsInformation.updateNumberOfTuplesRemovedByFullReducer(multiwayJoinParentName,
                        getDomainOriginalSize(multiwayJoinParentOperator) - multiwayJoinParentReducedDomainAsRowSet.size());
                statisticsInformation.updateSummationOfSemijoinOutputSize(multiwayJoinParentReducedDomainAsRowSet.size());
                statisticsInformation.updateNumberOfHashTableProbe(aggregateStatisticsInformation.getNumberOfHashTableProbe());
                statisticsInformation.updateNumberOfHashTableBuildTuples(aggregateStatisticsInformation.innerRelationSize);
                costMap.get(multiwayJoinParentNode).set(1, multiwayJoinParentReducedDomainAsRowSet.size());
            }
            if (multiwayJoinParentReducedDomainAsRowSet.size() == 0) {
                if (Switches.STATS) {
                    ((FullReducerStatisticsInformation) statisticsInformation)
                            .setSemijoinCausingEarlyStop(org.apache.commons.lang3.tuple.Pair.of(((JoinNode) plan.getRoot()).getLeft().getOperator().getMultiwayJoinNode(), ((JoinNode) plan.getRoot()).getRight().getOperator().getMultiwayJoinNode()));
                }
                return true;
            }
            multiwayJoinParentDomain.setRowSetAsDomain(multiwayJoinParentReducedDomainAsRowSet);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("%s domain (after semijoin): %s",
                        multiwayJoinParentNode.getNodeName(),
                        multiwayJoinParentDomain)));
            }
            multiwayJoinParentDomain.setUseDomainAsSource(true);
            for (Operator operator : node2Operators.get(multiwayJoinParentNode.getNodeId())) {
                operator.setUseDomainAsSource(true);
            }

            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                List<String> useDomainAsSourceStatus = new ArrayList<>();
                for (List<Operator> operators : node2Operators.values()) {
                    Operator operator = operators.get(0);
                    useDomainAsSourceStatus.add(String.format("%s : operator(%s), domain(%s)",
                            operator.getMultiwayJoinNode(),
                            operator.getUseDomainAsSource(),
                            operator.getMultiwayJoinNode().getDomain().getUseDomainAsSource()));
                }
                traceLogger.trace(formatTraceMessage(Joiner.on(",").join(useDomainAsSourceStatus)));
            }
        }
        return false;
    }

    @Override
    public Row getNext()
    {
        // We add a "sink" operator (i.e., the operator right below full reducer operator, i.e.,
        // the join operator of a left-deep join plan) so that call getNext() is the same as call getNext() of
        // the join operator.
        if (canBeEarlyStopped) {
            return null;
        }
        return sinkOperator.getNext();
    }

    @Override
    public void close()
    {
        sinkOperator.close();
        cleanUp();
    }

    @Override
    public void setChildren(List<Operator> children)
    {
        requireNonNull(children, "children cannot be null");
        checkArgument(children.size() == 1, "children has to have size 1 because there is one sink operator");
        sinkOperator = requireNonNull(children.get(0), "sink node doesn't contain operator");
    }

    @Override
    public void reset()
    {
        throw new UnsupportedOperationException("reset is not supported in full reducer operator");
    }

    @Override
    public List<? extends ColumnHandle> getColumns()
    {
        return sinkOperator.getColumns();
    }

    @Override
    public OptType getOperatorType()
    {
        return operatorType;
    }

    @Override
    public String getTraceOperatorName()
    {
        return "full reducer" + getOperatorID();
    }

    public Operator getSinkOperator()
    {
        return sinkOperator;
    }

    public void setSemijoins(List<Plan> semijoins, boolean isBottomUpSemijoins)
    {
        if (isBottomUpSemijoins) {
            this.bottomUpSemijoins = semijoins;
        }
        else {
            this.topDownSemijoins = semijoins;
        }
    }

    public List<Plan> getBottomUpSemijoins()
    {
        return bottomUpSemijoins;
    }

    public List<Plan> getTopDownSemijoins()
    {
        return topDownSemijoins;
    }

    private void cleanUp()
    {
        for (List<Operator> operators : node2Operators.values()) {
            for (Operator operator : operators) {
                operator.setUseDomainAsSource(false);
                operator.getMultiwayJoinNode().getDomain().setUseDomainAsSource(false);
                operator.close();
            }
        }
    }

    private long getDomainOriginalSize(Operator operator)
    {
        if (operator.getUseDomainAsSource()) {
            return operator.getMultiwayJoinNode().getDomain().size();
        }
        else {
            return planBuildContext.getCatalogGroup().getTableCatalog(operator.getSchemaTableName()).getSize();
        }
    }

    public void setNode2Operators(Map<Integer, List<Operator>> node2Operators)
    {
        this.node2Operators = node2Operators;
    }

    public void setSkipTopDownSemijoins(boolean skipTopDownSemijoins)
    {
        this.skipTopDownSemijoins = skipTopDownSemijoins;
    }
}
