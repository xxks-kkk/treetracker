package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.renebergelt.test.Switches;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.TableScanVisitor.gatherTableScanNodes;

public class YannakakisAggregateStatisticsInformation
        extends AggregateStatisticsInformation
{
    private final Logger traceLogger = LogManager.getLogger(YannakakisAggregateStatisticsInformation.class.getName());

    // estimate cost of full reducer, which is also used as the cost of Yannakakis's algorithm.
    @JsonProperty("CostOfYannakakis")
    @Getter
    private long estimateCostOfFullReducer;
    @JsonIgnore
    private Map<MultiwayJoinNode, List<Integer>> costMap;
    @Getter
    private long summationOfSemijoinOutputSize;
    @Getter
    // After bottom-up pass, Yannakakis is early stopped due to one of the relations is completely empty
    private boolean earlyStoppedDueToBottomUpPass;
    @Getter
    // After top-down pass, Yannakakis is early stopped due to one of the relations is completely empty
    private boolean earlyStoppedDueToTopDownPass;
    @Getter
    private Pair<MultiwayJoinNode, MultiwayJoinNode> semijoinCausingEarlyStop;
    @JsonProperty("fullReducerTime (ms)")
    @Getter
    private long fullReducerTime;
    @JsonIgnore
    @Getter
    private List<List<MultiwayJoinNode>> bottomUpSemiJoinOrdering;
    @JsonIgnore
    @Getter
    private List<List<MultiwayJoinNode>> topDownSemiJoinOrdering;

    protected class YannakakisPrinter
            extends Printer
    {
        @Override
        public void visitFullReducerOperator(FullReducerOperator operator, Void context)
        {
            FullReducerStatisticsInformation statisticsInformation = (FullReducerStatisticsInformation) operator.getStatisticsInformation();
            Map<String, Long> tuplesRemovedForEachRelation = statisticsInformation.getNumberOfTuplesRemovedByFullReducer();
            for (Long tuplesRemoved : tuplesRemovedForEachRelation.values()) {
                totalTuplesRemoved += tuplesRemoved;
            }
            costMap = statisticsInformation.getCostMap();
            numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
            summationOfSemijoinOutputSize += statisticsInformation.getSummationOfSemijoinOutputSize();
            earlyStoppedDueToBottomUpPass = statisticsInformation.isEarlyStoppedDueToBottomUpPass();
            earlyStoppedDueToTopDownPass = statisticsInformation.isEarlyStoppedDueToTopDownPass();
            semijoinCausingEarlyStop = statisticsInformation.getSemijoinCausingEarlyStop();
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug(String.format("numberOfR1Assignments of full reducer: %s", numberOfR1Assignments));
            }
            bottomUpSemiJoinOrdering = obtainBottomUpSemJoinOrdering(operator.getBottomUpSemijoins());
            topDownSemiJoinOrdering = obtainBottomUpSemJoinOrdering(operator.getTopDownSemijoins());
            fullReducerTime = TimeUnit.NANOSECONDS.toMillis(statisticsInformation.getFullReducerTime());
            process(operator.getSinkOperator(), context);
        }
    }

    public void setEstimateCostOfFullReducer(Map<MultiwayJoinNode, List<Integer>> costMap)
    {
        // It's possible that costMap can be null due to early stop.
        if (costMap != null) {
            for (MultiwayJoinNode node : costMap.keySet()) {
                List<Integer> costs = costMap.get(node);
                if (node.getNodeType() == NodeType.Internal) {
                    estimateCostOfFullReducer += 2L * costs.get(1);
                }
                estimateCostOfFullReducer += (costs.get(0) + costs.get(1));
            }
        }
    }

    @Override
    public void printHelper(Operator rootOperator)
    {
        Printer printer = new YannakakisPrinter();
        printer.process(rootOperator, null);
        setEstimateCostOfFullReducer(costMap);
        totalInputSizeAfterEvaluation = innerRelationSize + rkRelationSize - totalTuplesRemoved;
        // FIXME: for TPC-H Q15, DuckDB produces incorrect result for the view evaluation tpch.q15w_lineitem, which triggers
        // the checkState failure. We can try to upgrade DuckDB to see if the newer version works.
//        checkState(totalIntermediateResultsProducedWithoutNULL > 0, "totalIntermediateResultsProducedWithoutNULL cannot be negative");
    }

    private List<List<MultiwayJoinNode>> obtainBottomUpSemJoinOrdering(List<Plan> bottomUpSemiJoins)
    {
        List<List<MultiwayJoinNode>> ret = new ArrayList<>();
        for (Plan semijoinPlan : bottomUpSemiJoins) {
            List<MultiwayJoinNode> semijoinOrder = new ArrayList<>();
            List<PlanNode> tableScanNodes = gatherTableScanNodes(semijoinPlan.getRoot());
            for (PlanNode node : tableScanNodes) {
                semijoinOrder.add(node.getOperator().getMultiwayJoinNode());
            }
            ret.add(semijoinOrder);
        }
        return ret;
    }
}
