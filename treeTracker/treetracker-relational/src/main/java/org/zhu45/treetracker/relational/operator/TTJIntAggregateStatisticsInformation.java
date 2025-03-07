package org.zhu45.treetracker.relational.operator;

import org.zhu45.treetracker.relational.planner.plan.Side;

/**
 * This is only for testing purpose.
 */
public class TTJIntAggregateStatisticsInformation
        extends TTJAggregateStatisticsInformation
{
    @Override
    public void printHelper(Operator rootOperator)
    {
        OperatorVisitor<Void> printer = new OperatorVisitor<>()
        {
            @Override
            public void visitTupleBasedTableScanOperator(TupleBasedTableScanOperator operator, Void context)
            {
                if (operator.getSide() != Side.OUTER) {
                    numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                }
                else {
                    TupleBasedTableScanStatisticsInformation ttjStatisticsInformation = (TupleBasedTableScanStatisticsInformation) operator.getStatisticsInformation();
                    numberOfDanglingTuples += ttjStatisticsInformation.getNumberOfDanglingTuples();
                }
            }

            @Override
            void visitTupleBasedTreeTrackerOneBetaHashTableIntOperator(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator operator, Void context)
            {
                numberOfR1Assignments += operator.getStatisticsInformation().getNumberOfR1Assignments();
                numberOfDanglingTuples += operator.getStatisticsInformation().getNumberOfDanglingTuples();
                process(operator.r1Operator, context);
                process(operator.r2Operator, context);
            }
        };
        printer.process(rootOperator, null);
    }
}
