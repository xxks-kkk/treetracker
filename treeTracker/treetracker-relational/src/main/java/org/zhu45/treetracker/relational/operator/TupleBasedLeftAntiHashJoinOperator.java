package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;

public class TupleBasedLeftAntiHashJoinOperator
        extends TupleBasedLeftSemiHashJoinOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedLeftAntiHashJoinOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }

        while (true) {
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("(%s) numberOfR1Assignments after increment: %s",
                            getOperatorID(),
                            statisticsInformation.getNumberOfR1Assignments())));
                }
            }
            if (r1 == null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + null));
                    decrementTraceDepth();
                }
                return null;
            }
            if (!lookUpH()) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + r1));
                    decrementTraceDepth();
                }
                return r1;
            }
        }
    }
}
