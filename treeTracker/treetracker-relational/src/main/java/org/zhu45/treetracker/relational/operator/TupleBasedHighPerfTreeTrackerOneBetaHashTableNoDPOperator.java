package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.OptType;

/**
 * TTJ implementation but disabling deletion propagation.
 */
public class TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator
        extends TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedHighPerfTreeTrackerOneBetaHashTableNoDPOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @Override
    protected Row getR1(boolean getNewR1)
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
                    if (Switches.STATS) {
                        stopWatch.reset();
                        stopWatch.start();
                    }
                    Row ret = join(r1, currRowPointedbyIL);
                    if (Switches.STATS) {
                        stopWatch.stop();
                        statisticsInformation.updateJoinTime(stopWatch.getNanoTime());
                    }
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("return " + ret));
                        decrementTraceDepth();
                    }
                    return ret;
                }
            }
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1)));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            l = null;
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
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
            if (Switches.STATS) {
                stopWatch.reset();
                stopWatch.start();
            }
            lookUpHNew();
            if (Switches.STATS) {
                stopWatch.stop();
                statisticsInformation.updateJoinTime(stopWatch.getNanoTime());
            }
            r2 = lookUpHResult.row;
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r2 = " + r2));
            }
            if (r2 != null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + r2));
                    decrementTraceDepth();
                }
                return r2;
            }
            if (lookUpHResult.initiateNoGoodMarking) {
                if (Switches.STATS) {
                    statisticsInformation.incrementNumberOfInitPassContextCalls();
                }
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("r1 attributes: " + r1.getAttributes()));
                    if (r1Operator.getOperatorType() == OptType.table) {
                        traceLogger.trace(formatTraceMessage("r1Operator: " + r1Operator.getMultiwayJoinNode().getSchemaTableName()));
                    }
                    else {
                        traceLogger.trace(formatTraceMessage("r1Operator: " + r1Operator.getOperatorName()));
                    }
                    traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
                    traceLogger.trace(formatTraceMessage("initiate PassContext: " + context));
                }
                r1 = r1Operator.passContext(context);
            }
            else {
                r1 = r1Operator.getNext();
            }
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1)));
                    traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
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
