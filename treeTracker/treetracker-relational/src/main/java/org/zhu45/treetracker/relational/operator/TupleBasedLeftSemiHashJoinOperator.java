package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import static java.util.Objects.isNull;

public class TupleBasedLeftSemiHashJoinOperator
        extends TupleBasedHashJoinOperator
{
    private Logger traceLogger;

    public TupleBasedLeftSemiHashJoinOperator()
    {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedLeftSemiHashJoinOperator.class.getName());
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
            if (lookUpH()) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + r1));
                    decrementTraceDepth();
                }
                if (Switches.STATS) {
                    statisticsInformation.incrementNumberOfSemiJoinSuccess();
                }
                return r1;
            }
        }
    }

    protected boolean lookUpH()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".lookUpH()"));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " is l null? " + isNull(l)));
            incrementTraceDepth();
        }
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfHashTableProbe();
        }
        JoinValueContainerKey jav = extract(r1, true);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + jav));
        }
        l = hashTableH.get(jav);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("l = " + l));
        }
        if (l != null) {
            return true;
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return null"));
            decrementTraceDepth();
        }
        return false;
    }
}
