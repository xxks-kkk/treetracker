package org.zhu45.treetracker.relational.operator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.row.Row;

import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

public class TupleBasedNestedLoopJoinOperator
        extends TupleBasedJoinOperator
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(TupleBasedNestedLoopJoinOperator.class);
    private Logger traceLogger = LogManager.getLogger(TupleBasedNestedLoopJoinOperator.class.getName());

    @Override
    public Row getNext()
    {
        traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
        incrementTraceDepth();
        log.debug("operator id: " + getOperatorID());
        while (true) {
            r2 = r2Operator.getNext();
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r2 = " + r2));
            log.debug("r2 in getNext(): " + r2);
            if (r2 == null) {
                r1 = r1Operator.getNext();
                log.debug("r1 in getNext(): " + r1);
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
                if (r1 == null) {
                    traceLogger.trace(formatTraceMessage("return null"));
                    decrementTraceDepth();
                    return null;
                }
                r2Operator.reset();
                traceLogger.trace(formatTraceMessage("R2 reset()"));
                r2 = r2Operator.getNext();
                log.debug("r2 in getNext(): " + r2);
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r2 = " + r2));
            }
            if (r1 != null && r2 != null) {
                // Indeed, we can reuse ObjectRow.join(r1, r2) to prevent reinvent wheel.
                // Doing so, we can get rid of construct() step and the joinIdx, processedRelationRightColumn, and
                // resultColumnHandles in the TupleBasedJoinOperator.
                // The problem is that we recompute joinIdx each time we perform join, which is performance bad.
                Row res = join(r1, r2);
                if (res != null) {
                    traceLogger.trace(formatTraceMessage("return " + res));
                    decrementTraceDepth();
                    return res;
                }
            }
        }
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedNestedLoopJoinOperator(this, context);
    }
}
