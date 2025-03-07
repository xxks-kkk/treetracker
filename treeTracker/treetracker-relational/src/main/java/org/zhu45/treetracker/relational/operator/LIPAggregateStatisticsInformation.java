package org.zhu45.treetracker.relational.operator;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.relational.planner.plan.Side;

import java.util.concurrent.TimeUnit;

public class LIPAggregateStatisticsInformation
        extends AggregateStatisticsInformation
{
    @JsonIgnore
    private Logger traceLogger = LogManager.getLogger(LIPAggregateStatisticsInformation.class.getName());

    @Getter
    @JsonProperty("bloomFiltersProbingTime (ms)")
    // Bloom filters probing time
    private long bloomFiltersProbingTime;
    @Getter
    @JsonProperty("buildBloomFiltersTime (ms)")
    // time takes to build Bloom filters
    private long buildBloomFiltersTime;

    @Override
    public void printHelper(Operator rootOperator)
    {
        class LIPPrinter
                extends Printer
        {
            @Override
            public void visitTupleBasedLIPTableScanOperator(TupleBasedLIPTableScanOperator operator, Void context)
            {
                if (operator.getSide() != Side.OUTER) {
                    numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                    innerRelationSize += operator.getStatisticsInformation().getNumberOfTuples();
                }
                else {
                    rkRelationSize = operator.getStatisticsInformation().getNumberOfTuples();
                }
                totalTuplesRemoved += operator.getStatisticsInformation().getNumberOfTuplesFilteredOutByBloomFilters();
                bloomFiltersProbingTime = TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getBloomFiltersProbingTime());
                // NOTE: we don't need to update totalIntermediateResultsProduced (numberOfR1Assignments) because we already count
                // the number of tuples from R_k in the lowest join operator totalIntermediateResultsProduced (numberOfR1Assignments)
            }

            @Override
            public void visitTupleBasedLIPHashJoinOperator(TupleBasedLIPHashJoinOperator operator, Void context)
            {
                numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                totalIntermediateResultsProduced += operator.getStatisticsInformation().getNumberOfR1Assignments();
                totalIntermediateResultsProducedWithoutNULL += operator.getStatisticsInformation().getNumberOfR1Assignments() - 1;
                buildBloomFiltersTime += TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getBuildBloomFiltersTime());
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }
        }
        Printer printer = new LIPPrinter();
        printer.process(rootOperator, null);
        totalInputSizeAfterEvaluation = innerRelationSize + rkRelationSize - totalTuplesRemoved;
    }
}
