package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.row.Row;

import static java.util.Objects.requireNonNull;

/**
 * TTJ^{bf} = HJ + deleteDT() feature. This is a special operator used in Exp2.3: Effectiveness of Algorithmic Features
 * in TTJ.
 */
public class TreeTrackerBFJoinOperator
        extends TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TreeTrackerBFJoinOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
        }
        if (Switches.STATS) {
            stopWatch.reset();
            stopWatch.start();
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        Context context = (Context) info;
        MultiwayJoinNode currNode = context.getJoinNode();
        MultiwayJoinNode r2Node = r2Operator.getMultiwayJoinNode();
        MultiwayJoinOrderedGraph multiwayJoinOrderedGraph = planBuildContext.getOrderedGraph();
        if (!multiwayJoinOrderedGraph.getParent().get(currNode).get(0).equals(r2Node)) {
            r1 = r1Operator.passContext(info);
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1)));
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
        return getR1(false);
    }
}
