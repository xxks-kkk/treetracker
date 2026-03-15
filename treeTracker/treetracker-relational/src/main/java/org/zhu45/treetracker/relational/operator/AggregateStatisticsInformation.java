package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import de.renebergelt.test.Switches;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.relational.planner.plan.Side;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Data
public class AggregateStatisticsInformation
{
    @JsonIgnore
    private Logger traceLogger = LogManager.getLogger(AggregateStatisticsInformation.class.getName());

    String algorithm;
    String queryName;
    int numRelations;
    long resutSetSize;
    long totalTuplesRemoved;
    long numberOfDanglingTuples;
    @JsonProperty("runtime (ms)")
    long runtime;
    @JsonProperty("simpleCostModelCost")
    long numberOfR1Assignments;
    @JsonProperty("cost estimatation (cost model 4 weak assumption)")
    BigDecimal costModel4WeakCost;
    @JsonProperty("R_k tupleFetchingTime (ms)")
    long tupleFetchingTime;
    @JsonProperty("tupleFetchingTime for all table scan operators (ms)")
    long totalTupleFetchingTime;
    // This is similar to the numberOfR1Assignments (aka. simple cost model) but
    // we only count the number of R1 assignment to all the join operators + R_k
    long totalIntermediateResultsProduced;
    // Like totalIntermediateResultsProduced but don't count returned null that indicating end of the computation
    long totalIntermediateResultsProducedWithoutNULL;
    @JsonProperty("totalJoinTime (ms)")
    long totalJoinTime;
    @JsonProperty("hashTableBuildTime (ms)")
    long hashTableBuildTime;
    @JsonProperty("probeHashTableTime (ms)")
    long probeHashTableTime;
    @JsonProperty("predicateEvaluationTime (ms)")
    long predicateEvaluationTime;
    // The size of inner relations before the evaluation starts (original relation size)
    long innerRelationSize;
    // The size of R_k before the evaluation starts
    long rkRelationSize;
    // the total input size after evaluation finishes
    long totalInputSizeAfterEvaluation;
    // estimated memory consumption used by a query
    long evaluationMemoryCostInBytes;
    // number of hash table probe
    long numberOfHashTableProbe;
    // total domain size of inner relations
    long innerDomainSize;
    // relation sizes
    HashMap<String, Long> relationSizes = new HashMap<>();

    @JsonIgnore
    public String[] getHeader()
    {
        return new String[] {"Algorithm", "queryName", "numRelations", "resultSetSize", "totalTupesRemoved"};
    }

    @JsonIgnore
    public List<Object> getVals()
    {
        return List.of(algorithm, queryName, numRelations, resutSetSize, totalTuplesRemoved);
    }

    protected class Printer
            extends OperatorVisitor<Void>
    {
        @Override
        public void visitTupleBasedTreeTrackerOneBetaHashTableOperator(TupleBaseTreeTrackerOneBetaHashTableOperator operator, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visitTupleBasedTableScanOperator(TupleBasedTableScanOperator operator, Void context)
        {
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug("operator: " + operator.getOperatorID());
            }
            if (operator.getSide() != Side.OUTER) {
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    traceLogger.debug(String.format("Add %s from operator: %s",
                            operator.getStatisticsInformation().getNumberOfR1Assignments(),
                            operator.getTraceOperatorName()));
                }
                numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                innerRelationSize += operator.getStatisticsInformation().getNumberOfTuples();
                relationSizes.put(operator.getSchemaTableName().toString(), operator.getStatisticsInformation().getNumberOfTuples());
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    traceLogger.debug(String.format("numberOfR1Assignments increased to %s by %s due to %s",
                            numberOfR1Assignments,
                            operator.getStatisticsInformation().getNumberOfR1Assignments(),
                            operator.getTraceOperatorName()));
                }
                innerDomainSize += operator.getStatisticsInformation().getDomainSize();
            }
            else {
                traceLogger.debug("LeftMostPlanNodeOperator: " + operator.getOperatorID());
                tupleFetchingTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getFetchingTuplesTime());
                rkRelationSize = operator.getStatisticsInformation().getNumberOfTuples();
                relationSizes.put(operator.getSchemaTableName().toString(), operator.getStatisticsInformation().getNumberOfTuples());
            }
            totalTupleFetchingTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getFetchingTuplesTime());
            predicateEvaluationTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getPredicateEvaluationTime());
        }

        @Override
        public void visitTupleBasedNestedLoopJoinOperator(TupleBasedNestedLoopJoinOperator operator, Void context)
        {
            numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
            process(operator.r1Operator, context);
            process(operator.r2Operator, context);
        }

        @Override
        public void visitTupleBasedLeftSemiBloomJoinOperator(TupleBasedLeftSemiBloomJoinOperator operator, Void context)
        {
            numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
            totalIntermediateResultsProduced += operator.getStatisticsInformation().getNumberOfR1Assignments();
            totalIntermediateResultsProducedWithoutNULL += operator.getStatisticsInformation().getNumberOfR1Assignments() - 1;
            process(operator.r1Operator, context);
            process(operator.r2Operator, context);
        }

        @Override
        public void visitTupleBasedHashJoinOperator(TupleBasedHashJoinOperator operator, Void context)
        {
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug(String.format("Add numberOfR1Assignments: %s from operator: %s",
                        operator.getStatisticsInformation().getNumberOfR1Assignments(),
                        operator.getTraceOperatorName()));
                traceLogger.debug(String.format("numberOfR1Assignments increased from %s by %s due to %s",
                        numberOfR1Assignments,
                        operator.getStatisticsInformation().getNumberOfR1Assignments(),
                        operator.getTraceOperatorName()));
                traceLogger.debug(String.format("totalIntermediateResultsProduced increased from %s by %s due to %s",
                        numberOfR1Assignments,
                        operator.getStatisticsInformation().getNumberOfR1Assignments(),
                        operator.getTraceOperatorName()));
            }
            numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
            totalIntermediateResultsProduced += operator.getStatisticsInformation().getNumberOfR1Assignments();
            totalIntermediateResultsProducedWithoutNULL += operator.getStatisticsInformation().getNumberOfR1Assignments() - 1;
            totalJoinTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getJoinTime());
            hashTableBuildTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getHashTableBuildTime());
            probeHashTableTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getProbeHashTableTime());
            numberOfHashTableProbe += operator.getStatisticsInformation().getNumberOfHashTableProbe();
            process(operator.r1Operator, context);
            process(operator.r2Operator, context);
        }

        @Override
        public void visitTupleBasedLIPTableScanOperator(TupleBasedLIPTableScanOperator operator, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visitTupleBasedLIPHashJoinOperator(TupleBasedLIPHashJoinOperator operator, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void visitFullReducerOperator(FullReducerOperator operator, Void context)
        {
            throw new UnsupportedOperationException();
        }
    }

    public void printHelper(Operator rootOperator)
    {
        Printer printer = new Printer();
        printer.process(rootOperator, null);
        totalInputSizeAfterEvaluation = innerRelationSize + rkRelationSize - totalTuplesRemoved;
    }
}
