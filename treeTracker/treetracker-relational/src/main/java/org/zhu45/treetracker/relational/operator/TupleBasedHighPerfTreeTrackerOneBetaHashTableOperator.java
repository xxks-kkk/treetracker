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

public class TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator
        extends TupleBaseTreeTrackerOneBetaHashTableOperator
{
    private static final Logger traceLogger;

    protected Context context;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private static final LookUpHResult lookUpHResult = new LookUpHResult(null, false);

    public TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator()
    {
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
            if (hashTableH.isEmpty()) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".H = {}"));
                    traceLogger.trace(formatTraceMessage("return null"));
                    decrementTraceDepth();
                }
                return null;
            }
            if (Switches.STATS) {
                stopWatch.stop();
                statisticsInformation.updatePassContextWorkTime(stopWatch.getNanoTime());
                statisticsInformation.updateDeleteDanglingTupleFromHTime(System.nanoTime() - context.getPassContextInitiationTime());
            }
        }
        else {
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

    private void lookUpHNew()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".lookUpH()"));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " is l null? " + isNull(l)));
            incrementTraceDepth();
        }
        if (l == null || l.isEmpty()) {
            long probeHashTableTime;
            if (Switches.STATS) {
                probeHashTableTime = System.nanoTime();
            }
            JoinValueContainerKey jav = extract(r1, true);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            l = hashTableH.get(jav);
            if (Switches.STATS) {
                statisticsInformation.updateProbeHashTableTime(System.nanoTime() - probeHashTableTime);
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("l = " + l));
            }
            if (l != null) {
                iL = l.iterator();
                if (iL.hasNext()) {
                    currRowPointedbyIL = iL.next();
                    Row ret = join(r1, currRowPointedbyIL);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage(String.format("return (%s, %s)", ret, false)));
                        decrementTraceDepth();
                    }
                    lookUpHResult.row = ret;
                    lookUpHResult.initiateNoGoodMarking = false;
                    return;
                }
            }
            else {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return (null, true)"));
                    decrementTraceDepth();
                }
                lookUpHResult.row = null;
                lookUpHResult.initiateNoGoodMarking = true;
                return;
            }
        }
        if (iL.hasNext()) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("iL.hasNext() = true"));
            }
            currRowPointedbyIL = iL.next();
            Row ret = join(r1, currRowPointedbyIL);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("return (%s, %s)", ret, false)));
                decrementTraceDepth();
            }
            lookUpHResult.row = ret;
            lookUpHResult.initiateNoGoodMarking = false;
        }
        else {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("return (null, false)"));
                decrementTraceDepth();
            }
            lookUpHResult.row = null;
            lookUpHResult.initiateNoGoodMarking = false;
        }
    }

    public static class LookUpHResult
    {
        Row row;
        boolean initiateNoGoodMarking;

        public LookUpHResult(Row row, boolean initiateNoGoodMarking)
        {
            this.row = row;
            this.initiateNoGoodMarking = initiateNoGoodMarking;
        }
    }

    public static class Context
            implements OperatorInformation
    {
        private final MultiwayJoinNode joinNode;
        private final long passContextInitiationTime;

        public Context(MultiwayJoinNode node)
        {
            if (Switches.DEBUG) {
                requireNonNull(node, "node is null");
            }
            joinNode = node;
            passContextInitiationTime = System.nanoTime();
        }

        @Override
        public String toString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("(");
            if (joinNode != null) {
                stringBuilder.append("joinNode=")
                        .append(joinNode.getSchemaTableName().toString())
                        .append(",");
            }
            stringBuilder.append(")");
            return stringBuilder.toString();
        }

        public MultiwayJoinNode getJoinNode()
        {
            return joinNode;
        }

        public long getPassContextInitiationTime()
        {
            return passContextInitiationTime;
        }
    }

    @Override
    public void initializeContextObject()
    {
        context = new Context(r2Operator.getMultiwayJoinNode());
    }
}
