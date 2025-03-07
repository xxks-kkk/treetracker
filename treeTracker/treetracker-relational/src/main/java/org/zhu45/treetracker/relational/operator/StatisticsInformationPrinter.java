package org.zhu45.treetracker.relational.operator;

import org.zhu45.treetracker.common.Domain;
import org.zhu45.treetracker.relational.planner.plan.Side;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Print statistics of each operator in the plan
 */
public class StatisticsInformationPrinter
{
    public StatisticsInformationPrinter()
    {
    }

    public String print(Operator rootOperator)
    {
        StringBuilder output = new StringBuilder();
        printHelper(rootOperator, output);
        return output.toString();
    }

    private void printHelper(Operator rootOperator, StringBuilder output)
    {
        OperatorVisitor<Void> printer = new OperatorVisitor<>()
        {
            @Override
            public void visitTupleBasedTreeTrackerOneBetaHashTableOperator(TupleBaseTreeTrackerOneBetaHashTableOperator operator, Void context)
            {
                output.append(String.format("TTJ operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()))
                        .append("numberOfNoGoodTuples: ")
                        .append(operator.getStatisticsInformation().getNumberOfNoGoodTuples())
                        .append("\n")
                        .append("numberOfR1Assignments: ")
                        .append(operator.getStatisticsInformation().getNumberOfR1Assignments())
                        .append("\n")
                        .append("joinTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getJoinTime()))
                        .append("\n")
                        .append("passContextWorkTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getPassContextWorkTime()))
                        .append("\n")
                        .append("deleteDanglingTupleFromHTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getDeleteDanglingTupleFromHTime()))
                        .append("\n")
                        .append("hashTableBuildTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getHashTableBuildTime()))
                        .append("\n")
                        .append("probeHashTableTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getProbeHashTableTime()))
                        .append("\n")
                        .append("hashTableAllocationInitialCapacity: ")
                        .append(operator.getStatisticsInformation().getHashTableInitialAllocationCapacity())
                        .append("\n")
                        .append("numberOfPassContextCalls: ")
                        .append(operator.getStatisticsInformation().getNumberOfPassContextCalls())
                        .append("\n")
                        .append("numberOfInitPassContextCalls: ")
                        .append(operator.getStatisticsInformation().getNumberOfInitPassContextCalls())
                        .append("\n")
                        .append("-----\n");
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            void visitTupleBasedTreeTrackerOneBetaHashTableIntOperator(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator operator, Void context)
            {
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            public void visitTupleBasedTableScanOperator(TupleBasedTableScanOperator operator, Void context)
            {
                output.append(String.format("Table scan operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()));
                output.append("fetchingTuplesTime (ms): ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getFetchingTuplesTime()))
                        .append("\n");
                output.append("RecordTupleSourceClazzName: ")
                        .append(operator.getStatisticsInformation().getRecordTupleSourceClazzName())
                        .append("\n");
                if (operator.getSide() == Side.OUTER && operator.getNoGoodListMap() != null) {
                    output.append("noGoodListClazz: ")
                            .append(operator.getNoGoodListMap().getClass().getSimpleName())
                            .append("\n");
                    output.append("numberOfNoGoodTuples: ")
                            .append(operator.getNoGoodListMap().getStatisticsInformation().getNumberOfNoGoodTuples())
                            .append("\n");
                    output.append("numberOfNoGoodTuplesFiltered: ")
                            .append(operator.getNoGoodListMap().getStatisticsInformation().getNumberOfNoGoodTuplesFiltered())
                            .append("\n");
                    output.append("noGoodListMapRepresentation: ")
                            .append(operator.getNoGoodListMap().getStatisticsInformation().getNoGoodListMapRepresentation())
                            .append("\n");
                    output.append("noGoodListProbingTime (ms): ")
                            .append(TimeUnit.NANOSECONDS.toMillis(operator.getNoGoodListMap().getStatisticsInformation().getNoGoodListProbingTime()))
                            .append("\n");
                    output.append("buildNoGoodListTime (ms): ")
                            .append(TimeUnit.NANOSECONDS.toMillis(operator.getNoGoodListMap().getStatisticsInformation().getBuildNoGoodListTime()))
                            .append("\n");
                    output.append("noGoodListConstructTime (ms): ")
                            .append(TimeUnit.NANOSECONDS.toMillis(operator.getNoGoodListMap().getStatisticsInformation().getNoGoodListConstructTime()))
                            .append("\n");
                    output.append("numberOfPassContextCalls: ")
                            .append(operator.getStatisticsInformation().getNumberOfPassContextCalls())
                            .append("\n");
                }
                output.append("predicateEvaluationTime (ms): ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getPredicateEvaluationTime()))
                        .append("\n");
                Domain domain = operator.getStatisticsInformation().getDomain();
                if (domain != null) {
                    output.append("Domain: ")
                            .append(domain)
                            .append("(")
                            .append(operator.getStatisticsInformation().getDomainSize())
                            .append(")")
                            .append("\n");
                }
                output.append("-----\n");
            }

            @Override
            public void visitTupleBasedNestedLoopJoinOperator(TupleBasedNestedLoopJoinOperator operator, Void context)
            {
                output.append(String.format("NestedLoopJoin operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()));
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            public void visitTupleBasedLeftSemiBloomJoinOperator(TupleBasedLeftSemiBloomJoinOperator operator, Void context)
            {
                output.append(String.format("TupleBasedLeftSemiBloomJoinOperator operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()));
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            public void visitTupleBasedHashJoinOperator(TupleBasedHashJoinOperator operator, Void context)
            {
                output.append(String.format("HashJoin operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()))
                        .append("numberOfR1Assignments: ")
                        .append(operator.getStatisticsInformation().getNumberOfR1Assignments())
                        .append("\n")
                        .append("joinTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getJoinTime()))
                        .append("\n")
                        .append("probeHashTableTime: ")
                        .append(TimeUnit.NANOSECONDS.toMillis(operator.getStatisticsInformation().getProbeHashTableTime()))
                        .append("\n")
                        .append("hashTableAllocationInitialCapacity: ")
                        .append(operator.getStatisticsInformation().getHashTableInitialAllocationCapacity())
                        .append("\n")
                        .append("-----\n");
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            public void visitTupleBasedLIPTableScanOperator(TupleBasedLIPTableScanOperator operator, Void context)
            {
                output.append(String.format("LIP Table scan operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()))
                        .append("numberOfTuplesFilteredOutByBloomFilters: ")
                        .append(operator.getStatisticsInformation().getNumberOfTuplesFilteredOutByBloomFilters())
                        .append("\n")
                        .append("numberOfBloomFiltersRegistered: ")
                        .append(operator.getStatisticsInformation().getNumberOfBloomFiltersRegistered())
                        .append("\n-----\n");
            }

            @Override
            public void visitTupleBasedLIPHashJoinOperator(TupleBasedLIPHashJoinOperator operator, Void context)
            {
                output.append(String.format("LIP Hash join operator (%s: %s): \n", operator.getOperatorID(), operator.getTraceOperatorName()))
                        .append("approxNumberOfTuplesInBloomFilter: ")
                        .append(operator.getStatisticsInformation().getApproxNumberOfTuplesInBloomFilter())
                        .append("\n")
                        .append("numberOfR1Assignments: ")
                        .append(operator.getStatisticsInformation().getNumberOfR1Assignments())
                        .append("\n-----\n");
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }

            @Override
            public void visitFullReducerOperator(FullReducerOperator operator, Void context)
            {
                output.append(String.format("full reducer operator (%s: %s):\n", operator.getOperatorID(), operator.getTraceOperatorName()));
                output.append("relation | [numberOfTuplesRemovedByFullReducer]")
                        .append("\n");
                Map<String, Long> numberOfTuplesRemovedByFullReducer = operator.getStatisticsInformation().getNumberOfTuplesRemovedByFullReducer();
                for (String relation : numberOfTuplesRemovedByFullReducer.keySet()) {
                    output.append(String.format("%s [%s]\n", relation, numberOfTuplesRemovedByFullReducer.get(relation)));
                }
                AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                        .setRootOperator(operator)
                        .setJoinOperator(JoinOperator.Yannakakis)
                        .build();
                AggregateStatisticsInformationFactory aggregateStatisticsInformationFactory =
                        new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
                YannakakisAggregateStatisticsInformation aggregateStatisticsInformation = (YannakakisAggregateStatisticsInformation) aggregateStatisticsInformationFactory.get();
                output.append("estimateCostOfFullReducer: ")
                        .append(aggregateStatisticsInformation.getEstimateCostOfFullReducer())
                        .append("\n");
                output.append("-----\n");
                process(operator.getSinkOperator(), context);
            }
        };
        printer.process(rootOperator, null);
    }
}
