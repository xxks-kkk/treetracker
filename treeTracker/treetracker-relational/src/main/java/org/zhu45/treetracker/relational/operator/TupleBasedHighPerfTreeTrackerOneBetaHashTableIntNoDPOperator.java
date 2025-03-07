package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.IntRow;

public class TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator
        extends TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @Override
    protected IntRow getR1(boolean getNewR1)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getR1(" + getNewR1 + ")"));
            incrementTraceDepth();
        }
        if (getNewR1) {
            if (l != null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " advance I_l"));
                }
                if (iL.hasNext()) {
                    currRowPointedbyIL = iL.next();
                    IntRow ret = join((IntRow) r1, currRowPointedbyIL);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("return " + ret));
                        decrementTraceDepth();
                    }
                    return ret;
                }
            }
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            l = null;
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
        }
        if (l != null && l.isEmpty()) {
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1)));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
            }
        }
        if (r1 == null) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("return " + null));
                decrementTraceDepth();
            }
            return null;
        }
        while (true) {
            lookUpHNew();
            r2 = lookUpHResult.row;
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r2 = " + r2));
            }
            if (r2 != null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + r2));
                    decrementTraceDepth();
                }
                return (IntRow) r2;
            }
            if (lookUpHResult.initiateNoGoodMarking) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("r1 attributes: " + r1.getAttributes()));
                    traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
                    traceLogger.trace(formatTraceMessage("initiate PassContext: " + this.parentNodeId));
                }
                r1 = r1Operator.passContext(this.parentNodeId, this.r2NodeId);
            }
            else {
                r1 = r1Operator.getNext();
            }
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s",
                        statisticsInformation.getNumberOfR1Assignments(),
                        statisticsInformation.getNumberOfR1Assignments() + 1)));
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
            }

            if (r1 == null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return null"));
                    decrementTraceDepth();
                }
                return null;
            }
            l = null;
        }
    }
}
