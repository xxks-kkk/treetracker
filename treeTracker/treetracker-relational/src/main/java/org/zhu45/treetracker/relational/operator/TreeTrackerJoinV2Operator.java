package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import java.util.List;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

public class TreeTrackerJoinV2Operator
        extends TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TreeTrackerJoinV2Operator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    public TreeTrackerJoinV2Operator()
    {
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
                incrementTraceDepth();
            }
        }
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        Context context = (Context) info;
        MultiwayJoinNode currNode = context.getJoinNode();
        MultiwayJoinNode r2Node = r2Operator.getMultiwayJoinNode();
        MultiwayJoinOrderedGraph multiwayJoinOrderedGraph = (MultiwayJoinOrderedGraph) planBuildContext.getOrderedGraph();
        if (multiwayJoinOrderedGraph.getParent().get(currNode).get(0).equals(r2Node)) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("%s is the parent of %s in G", r2Node, currNode)));
            }
            JoinValueContainerKey jav = extract(currRowPointedbyIL, false);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("to be removed from H: " + currRowPointedbyIL));
            }
            iL.remove();
            if (Switches.STATS) {
                ((TTJStatisticsInformation) statisticsInformation).incrementNodeToNoGoodTuples(currNode);
                statisticsInformation.incrementNumberOfNoGoodTuples();
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("incrementNodeToNoGoodTuples: " + ((TTJStatisticsInformation) statisticsInformation).getNodeToNoGoodTuples()));
                }
            }
            List<Row> l = hashTableH.get(jav);
            if (l.isEmpty()) {
                hashTableH.remove(jav);
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".H = " + hashTableH));
            }
        }
        else {
            r1 = r1Operator.passContext(info);
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s in %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1,
                            getTraceOperatorName())));
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
        return getNext();
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }

        if (l != null && !l.isEmpty()) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " advance I_l"));
                traceLogger.trace(formatTraceMessage("l: " + l));
            }
            if (iL.hasNext()) {
                currRowPointedbyIL = iL.next();
                Row ret = join(r1, currRowPointedbyIL);
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + ret));
                    decrementTraceDepth();
                }
                return ret;
            }
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s in %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1,
                            getTraceOperatorName())));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            if (r1 == null) {
                return null;
            }
        }
        if (r1 == null) {
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s in %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1,
                            getTraceOperatorName())));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
            }
        }

        while (r1 != null) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r1 = " + r1));
            }
            l = lookUpHashTable();
            if (l != null) {
                iL = l.iterator();
                if (iL.hasNext()) {
                    currRowPointedbyIL = iL.next();
                    Row ret = join(r1, currRowPointedbyIL);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("return " + ret));
                        decrementTraceDepth();
                    }
                    return ret;
                }
            }
            else {
                if (Switches.STATS) {
                    statisticsInformation.incrementNumberOfInitPassContextCalls();
                }
                r1 = r1Operator.passContext(context);
                if (Switches.STATS) {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s in %s",
                                statisticsInformation.getNumberOfR1Assignments(),
                                statisticsInformation.getNumberOfR1Assignments() + 1,
                                getTraceOperatorName())));
                    }
                    statisticsInformation.incrementNumberOfR1Assignments();
                }
            }
        }
        return null;
    }

    private List<Row> lookUpHashTable()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".lookUpHashTable()"));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " is l null? " + isNull(l)));
            incrementTraceDepth();
        }
        JoinValueContainerKey jav = extract(r1, true);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + jav));
        }
        List<Row> ret = hashTableH.get(jav);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("ret = " + ret));
        }
        return ret;
    }
}
