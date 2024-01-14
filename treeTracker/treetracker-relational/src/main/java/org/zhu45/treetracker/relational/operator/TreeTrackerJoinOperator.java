package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import java.util.List;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/**
 * In this implementation, we explore a TTJ variant that aims for better
 * presentation of TTJ.
 * <p>
 * This is an experiment version and may not be the final TTJ to be used.
 * This version has different semantics than the original TTJ, i.e.,
 * TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator. Please see
 * TestTTJAggregateStatisticsInformationWithTTJOperator for example.
 */
public class TreeTrackerJoinOperator
        extends TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator
{
    private Logger traceLogger;

    public TreeTrackerJoinOperator()
    {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TreeTrackerJoinOperator.class.getName());
        }
    }

    @Override
    public ObjectRow passContext(OperatorInformation info)
    {
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
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
            if (Switches.DEBUG) {
                ((TTJStatisticsInformation) statisticsInformation).incrementNodeToNoGoodTuples(currNode);
                statisticsInformation.incrementNumberOfNoGoodTuples();
                if (traceLogger.isTraceEnabled()) {
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
            l = null;
            r1Operator.passContext(info);
        }
        return null;
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }

        if (l != null) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " advance I_l"));
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
        }

        while ((r1 = r1Operator.getNext()) != null) {
            if (Switches.DEBUG) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
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
                r1Operator.passContext(context);
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
