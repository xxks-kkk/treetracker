package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.Row;

import java.util.List;

import static java.util.Objects.isNull;

/***
 * A special case of TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator where Row consists of all integers. No statistics will be collected inside
 * this operator. If statistics is needed, use TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator instead.
 */
public class TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator
        extends TupleBasedIntHashJoinOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected static final LookUpHResult lookUpHResult = new LookUpHResult(null, false);

    protected int parentNodeId;
    protected int r2NodeId;
    private MultiwayJoinNode node;
    private MultiwayJoinNode childMultiwayJoinNode;

    public TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator()
    {
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }
        Row tmp = getR1(true);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return " + tmp));
            decrementTraceDepth();
        }
        return tmp;
    }

    @Override
    public IntRow passContext(int parentNodeId, int id)
    {
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + parentNodeId + ")"));
            incrementTraceDepth();
        }
        if (parentNodeId == r2NodeId) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("%s is the parent of %s in G", r2NodeId, parentNodeId)));
            }
            extract(currRowPointedbyIL, false);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("to be removed from H: " + currRowPointedbyIL));
            }
            iL.remove();
            List<IntRow> l = hashTableH.get(javR1);
            if (l.isEmpty()) {
                hashTableH.remove(javR1);
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
        }
        else {
            r1 = r1Operator.passContext(parentNodeId, id);
            if (Switches.STATS) {
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
                    join((IntRow) r1, currRowPointedbyIL);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("return " + joinResult));
                        decrementTraceDepth();
                    }
                    return joinResult;
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
            r1 = r1Operator.passContext(this.parentNodeId, this.r2NodeId);
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

    protected void lookUpHNew()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".lookUpH()"));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " is l null? " + isNull(l)));
            incrementTraceDepth();
        }
        if (l == null || l.isEmpty()) {
            extractR1((IntRow) r1);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("javR1: " + javR1));
            }
            l = hashTableH.get(javR1);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("l = " + l));
            }
            if (l != null) {
                iL = l.iterator();
                if (iL.hasNext()) {
                    currRowPointedbyIL = iL.next();
                    join((IntRow) r1, currRowPointedbyIL);
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage(String.format("return (%s, %s)", joinResult, false)));
                        decrementTraceDepth();
                    }
                    lookUpHResult.row = joinResult;
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
            join((IntRow) r1, currRowPointedbyIL);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(String.format("return (%s, %s)", joinResult, false)));
                decrementTraceDepth();
            }
            lookUpHResult.row = joinResult;
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
        IntRow row;
        boolean initiateNoGoodMarking;

        public LookUpHResult(IntRow row, boolean initiateNoGoodMarking)
        {
            this.row = row;
            this.initiateNoGoodMarking = initiateNoGoodMarking;
        }
    }

    @Override
    public void initializeContextObject()
    {
        parentNodeId = r2Operator.getChildMultiwayJoinNode() == null ?
                r2Operator.getPlanBuildContext().getOrderedGraph().getParent().get(r2Operator.getMultiwayJoinNode()).get(0).getNodeId() :
                planBuildContext.getOrderedGraph().getParent().get(r2Operator.getChildMultiwayJoinNode()).get(0).getNodeId();
        r2NodeId = r2Operator.getChildMultiwayJoinNode() == null ?
                r2Operator.getMultiwayJoinNode().getNodeId() :
                r2Operator.getChildMultiwayJoinNode().getNodeId();
    }

    @Override
    public void setMultiwayJoinNode(MultiwayJoinNode node)
    {
        this.node = node;
    }

    @Override
    public MultiwayJoinNode getMultiwayJoinNode()
    {
        return this.node;
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedTreeTrackerOneBetaHashTableIntOperator(this, context);
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        if (node != null) {
            return node.getSchemaTableName();
        }
        return null;
    }

    @Override
    public void setChildMultiwayJoinNode(MultiwayJoinNode node)
    {
        this.childMultiwayJoinNode = node;
    }

    @Override
    public MultiwayJoinNode getChildMultiwayJoinNode()
    {
        return this.childMultiwayJoinNode;
    }
}
