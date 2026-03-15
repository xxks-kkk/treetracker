package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import de.renebergelt.test.Switches;
import lombok.Data;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.plan.Side;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class TTJAggregateStatisticsInformation
        extends AggregateStatisticsInformation
{
    @JsonIgnore
    private Logger traceLogger = LogManager.getLogger(TTJAggregateStatisticsInformation.class.getName());

    private boolean ttjhpNoNGPlan;
    private Class<?> joinOperatorClazz;

    // the estimation cost of TTJ
    // TODO: need to refactor this logic into cost model 3
    @Getter
    long estimateCostOfTTJ;
    @JsonProperty("noGoodListProbingTime (ms)")
    @Getter
    long noGoodListProbingTime;
    @JsonProperty("passContextWorkTime (ms)")
    @Getter
    long passContextWorkTime;
    @JsonProperty("deleteDanglingTupleFromHTime (ms)")
    @Getter
    long deleteDanglingTupleFromHTime;
    @JsonProperty("buildNoGoodListTime (ms)")
    @Getter
    long buildNoGoodListTime;
    @JsonProperty("noGoodListConstructTime (ms)")
    @Getter
    long noGoodListConstructTime;
    @Getter
    long noGoodListSize;
    @Getter
    long noGoodListSizeInBytes;
    @Getter
    long numberOfPassContextCalls;
    @Getter
    long numberOfPassContextCallsInnerRelations;
    @Getter
    long numberOfPassContextCallsRk;
    @Getter
    long numberOfInitPassContextCalls;
    // Number of tuples filtered by no-good list
    @Getter
    long totalTuplesFiltered;
    // Number of tuples removed at R_k by adding tuples to no-good list
    @Getter
    long totalTuplesRemovedRk;
    // Number of tuples removed at inner relations
    @Getter
    long totalTuplesRemovedInnerRelations;
    @Getter long numberOfDeletionPropagationTriggered;

    @VisibleForTesting
    @Data
    static class CostEstimationContext
    {
        MultiwayJoinOrderedGraph multiwayJoinOrderedGraph;
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> aggregateNodeToNoGoodTuples = new HashMap<>();
        // noGoodListMap key size in R_k
        int noGoodListMapKeyNum;
        // number of tuples of R_k
        long numberOfTuplesForRk;
        // the root node of join tree
        MultiwayJoinNode rootNode;
    }

    private void updateEstimateCostOfTTJ(CostEstimationContext context)
    {
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr = generateDSR(context.multiwayJoinOrderedGraph);
        for (MultiwayJoinNode parent : context.aggregateNodeToNoGoodTuples.keySet()) {
            HashMap<MultiwayJoinNode, Integer> nodeToNoGoodTuples = context.aggregateNodeToNoGoodTuples.get(parent);
            if (!parent.equals(context.rootNode)) {
                for (MultiwayJoinNode child : nodeToNoGoodTuples.keySet()) {
                    estimateCostOfTTJ += (long) nodeToNoGoodTuples.get(child) * (1 + Math.max(1, dsr.get(parent).get(child)));
                }
            }
            else {
                for (MultiwayJoinNode child : nodeToNoGoodTuples.keySet()) {
                    estimateCostOfTTJ += (long) nodeToNoGoodTuples.get(child) * (1 + Math.max(1, dsr.get(parent).get(child)));
                }
            }
        }
        estimateCostOfTTJ += context.getNumberOfTuplesForRk() * context.noGoodListMapKeyNum;
    }

    /**
     * Compute d_R^S for all nodes in the graph. The result is a map with key denote a node and value is
     * a list containing Map<child node, the path length>. The path length is the number of nodes between the node (including)
     * and child node (excluding) in the pre-order traversal ordering.
     */
    public static HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> generateDSR(MultiwayJoinOrderedGraph orderedGraph)
    {
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> result = new HashMap<>();
        Queue<MultiwayJoinNode> roots = new LinkedList<>();
        Queue<MultiwayJoinNode> dfs = new LinkedList<>();
        roots.add(orderedGraph.getRoot());
        HashMap<MultiwayJoinNode, List<MultiwayJoinNode>> children = orderedGraph.getChildren();
        while (!roots.isEmpty()) {
            MultiwayJoinNode rootBaseNode = roots.poll();
            result.put(rootBaseNode, new HashMap<>());
            dfs.add(rootBaseNode);
            dfs.addAll(children.get(rootBaseNode));
            while (!dfs.isEmpty()) {
                MultiwayJoinNode child = dfs.poll();
                result.get(rootBaseNode).put(child, orderedGraph.getPathLength(rootBaseNode, child));
                roots.addAll(children.get(child));
            }
        }
        return result;
    }

    @Override
    public void printHelper(Operator rootOperator)
    {
        OperatorVisitor<CostEstimationContext> printer = new OperatorVisitor<>()
        {
            @Override
            public void visitTupleBasedTreeTrackerOneBetaHashTableOperator(TupleBaseTreeTrackerOneBetaHashTableOperator operator, CostEstimationContext context)
            {
                if (joinOperatorClazz == null) {
                    joinOperatorClazz = operator.getClass();
                }
                if (context.multiwayJoinOrderedGraph == null) {
                    context.multiwayJoinOrderedGraph = operator.getPlanBuildContext().getOrderedGraph();
                }
                totalTuplesRemoved += operator.getStatisticsInformation().getNumberOfNoGoodTuples();
                totalTuplesRemovedInnerRelations += operator.getStatisticsInformation().getNumberOfNoGoodTuples();
                numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                numberOfDanglingTuples += operator.getStatisticsInformation().getNumberOfDanglingTuples();
                totalIntermediateResultsProduced += operator.getStatisticsInformation().getNumberOfR1Assignments();
                //  -1 is because the count of returned null from GetNext() that indicates no more join result left
                totalIntermediateResultsProducedWithoutNULL += operator.getStatisticsInformation().getNumberOfR1Assignments() - 1;
                totalJoinTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getJoinTime());
                passContextWorkTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getPassContextWorkTime());
                hashTableBuildTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getHashTableBuildTime());
                deleteDanglingTupleFromHTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getDeleteDanglingTupleFromHTime());
                probeHashTableTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getProbeHashTableTime());
                numberOfPassContextCalls += operator.getStatisticsInformation().getNumberOfPassContextCalls();
                numberOfPassContextCallsInnerRelations += operator.getStatisticsInformation().getNumberOfPassContextCalls();
                numberOfInitPassContextCalls += operator.getStatisticsInformation().getNumberOfInitPassContextCalls();
                numberOfHashTableProbe += operator.getStatisticsInformation().getNumberOfHashTableProbe();
                numberOfDeletionPropagationTriggered += operator.getStatisticsInformation().getNumberOfDeletionPropagationTriggered();
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    traceLogger.debug(String.format("numberOfR1Assignments increased to %s by %s due to %s",
                            numberOfR1Assignments,
                            operator.getStatisticsInformation().getNumberOfR1Assignments(),
                            operator.getTraceOperatorName()));
                }
                TTJStatisticsInformation ttjStatisticsInformation = (TTJStatisticsInformation) operator.getStatisticsInformation();
                HashMap<MultiwayJoinNode, Integer> prev = context.aggregateNodeToNoGoodTuples.put(operator.r2Operator.getMultiwayJoinNode(), ttjStatisticsInformation.getNodeToNoGoodTuples());
                checkArgument(prev == null, String.format("%s already presents in aggregateNodeToNoGoodTuples", operator.r2Operator.getMultiwayJoinNode()));
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            public void visitTupleBasedTableScanOperator(TupleBasedTableScanOperator operator, CostEstimationContext context)
            {
                if (operator.getSide() != Side.OUTER) {
                    numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                    innerRelationSize += operator.getStatisticsInformation().getNumberOfTuples();
                    relationSizes.put(operator.getSchemaTableName().toString(), operator.getStatisticsInformation().getNumberOfTuples());
                    if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                        traceLogger.debug(String.format("numberOfR1Assignments increased to %s by %s due to %s",
                                numberOfR1Assignments,
                                operator.getStatisticsInformation().getNumberOfR1Assignments(),
                                operator.getTraceOperatorName()));
                    }
                }
                else {
                    context.rootNode = operator.getMultiwayJoinNode();
                    TupleBasedTableScanStatisticsInformation ttjStatisticsInformation = (TupleBasedTableScanStatisticsInformation) operator.getStatisticsInformation();
                    numberOfPassContextCalls += ttjStatisticsInformation.getNumberOfPassContextCalls();
                    numberOfPassContextCallsRk += ttjStatisticsInformation.getNumberOfPassContextCalls();
                    numberOfDanglingTuples += ttjStatisticsInformation.getNumberOfDanglingTuples();
                    tupleFetchingTime = TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getFetchingTuplesTime());
                    rkRelationSize = ttjStatisticsInformation.getNumberOfTuples();
                    relationSizes.put(operator.getSchemaTableName().toString(), operator.getStatisticsInformation().getNumberOfTuples());
                    context.numberOfTuplesForRk = ttjStatisticsInformation.getNumberOfTuples();

                    if (operator.getNoGoodListMap() != null) {
                        TupleBasedTableScanStatisticsInformation noGoodListMapStatisticsInformation = (TupleBasedTableScanStatisticsInformation) operator.getNoGoodListMap()
                                .getStatisticsInformation();
                        HashMap<MultiwayJoinNode, Integer> prev = context.aggregateNodeToNoGoodTuples.put(operator.getMultiwayJoinNode(),
                                noGoodListMapStatisticsInformation.getNodeToNoGoodTuples());
                        checkArgument(prev == null, String.format("%s already presents in aggregateNodeToNoGoodTuples", operator.getMultiwayJoinNode()));
                        context.noGoodListMapKeyNum = noGoodListMapStatisticsInformation.getNoGoodListMapKeyNum();

                        noGoodListProbingTime = TimeUnit.NANOSECONDS.toMillis(noGoodListMapStatisticsInformation.getNoGoodListProbingTime());
                        // NOTE: we don't need to update totalIntermediateResultsProduced (numberOfR1Assignments) because we already count
                        // the number of tuples from R_k in the lowest join operator totalIntermediateResultsProduced (numberOfR1Assignments)
                        buildNoGoodListTime += TimeUnit.NANOSECONDS.toMillis(noGoodListMapStatisticsInformation.getBuildNoGoodListTime());
                        noGoodListConstructTime += TimeUnit.NANOSECONDS.toMillis(noGoodListMapStatisticsInformation.getNoGoodListConstructTime());
                        noGoodListSizeInBytes = noGoodListMapStatisticsInformation.getNoGoodListSizeInBytes();
                        noGoodListSize = noGoodListMapStatisticsInformation.getNumberOfNoGoodTuples();
                        totalTuplesFiltered += noGoodListMapStatisticsInformation.getNumberOfNoGoodTuplesFiltered();
                        totalTuplesRemoved += noGoodListMapStatisticsInformation.getNumberOfNoGoodTuples();
                        totalTuplesRemovedRk += noGoodListMapStatisticsInformation.getNumberOfNoGoodTuples();
                    }
                    else {
                        ttjhpNoNGPlan = true;
                    }
                }
                totalTupleFetchingTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getFetchingTuplesTime());
                predicateEvaluationTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getPredicateEvaluationTime());
            }

            @Override
            public void visitTupleBasedNestedLoopJoinOperator(TupleBasedNestedLoopJoinOperator operator, CostEstimationContext context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visitTupleBasedHashJoinOperator(TupleBasedHashJoinOperator operator, CostEstimationContext context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visitTupleBasedLIPTableScanOperator(TupleBasedLIPTableScanOperator operator, CostEstimationContext context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visitTupleBasedLIPHashJoinOperator(TupleBasedLIPHashJoinOperator operator, CostEstimationContext context)
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public void visitFullReducerOperator(FullReducerOperator operator, CostEstimationContext context)
            {
                throw new UnsupportedOperationException();
            }
        };
        CostEstimationContext context = new CostEstimationContext();
        printer.process(rootOperator, context);
        updateEstimateCostOfTTJ(context);
        totalInputSizeAfterEvaluation = innerRelationSize + rkRelationSize -
                (totalTuplesRemoved + totalTuplesFiltered);
        validate();
    }

    /**
     * check the integrity of the stats collected for TTJ
     */
    private void validate()
    {
        checkState(numberOfPassContextCalls >= numberOfInitPassContextCalls,
                String.format("numberOfPassContextCalls (%s) should >= numberOfInitPassContextCalls (%s)",
                        numberOfPassContextCalls,
                        numberOfInitPassContextCalls));
        checkState(numberOfPassContextCalls == numberOfDanglingTuples,
                String.format("numberOfPassContextCalls (%s) should = numberOfDanglingTuples (%s)",
                        numberOfPassContextCalls,
                        numberOfDanglingTuples));
        if (!ttjhpNoNGPlan) {
            // The only exception is TTJHP_NO_NG plan
            checkState(numberOfInitPassContextCalls == totalTuplesRemoved,
                    String.format("numberOfInitPassContextCalls (%s) should = totalTuplesRemoved (%s)",
                            numberOfInitPassContextCalls, totalTuplesRemoved));
        }
        checkState(totalTuplesRemoved == totalTuplesRemovedRk + totalTuplesRemovedInnerRelations,
                String.format("totalTuplesRemoved (%s) should = totalTuplesRemovedRk (%s) + totalTuplesRemovedInnerRelations (%s)",
                        totalTuplesRemoved, totalTuplesRemovedRk, totalTuplesRemovedInnerRelations));
        checkState(totalInputSizeAfterEvaluation > 0, "totalInputSizeAfterEvaluation cannot be negative");
        checkState(noGoodListSize == totalTuplesRemovedRk,
                String.format("noGoodListSize (%s) should = totalTuplesRemovedRk (%s)",
                        noGoodListSize,
                        totalTuplesRemovedRk));
        if (joinOperatorClazz == TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class ||
                joinOperatorClazz == TupleBasedLeftSemiHashJoinOperator.class ||
                joinOperatorClazz == TupleBasedHashJoinOperator.class
                || joinOperatorClazz == TupleBasedIntHashJoinOperator.class
                || joinOperatorClazz == TupleBasedLeftSemiHashJoinIntOperator.class) {
            checkState(totalIntermediateResultsProducedWithoutNULL ==
                    numberOfHashTableProbe, String.format("totalIntermediateResultsProducedWithoutNULL(%s) should = numberOfHashTableProbe (%s)",
                    totalIntermediateResultsProducedWithoutNULL,
                    numberOfHashTableProbe));
        }
    }
}
