package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import java.util.List;

import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;

/***
 * We keep kr PassContext() marking calls for historical purpose. This version of TTJ
 * is for algorithm development. Thus, this file may be outdated, i.e., not reflect the latest
 * TTJ development.
 */
public class TupleBaseTreeTrackerOneBetaHashTableOperator
        extends TupleBasedHashJoinOperator
{
    private static final Logger traceLogger;

    private Context context;
    protected MultiwayJoinNode multiwayJoinNode;
    // the MultiwayJoinNode associated with the join operator when the join operator serves as an inner relation
    // of a left-deep plan that is above it.
    protected MultiwayJoinNode childMultiwayJoinNode;
    protected MultiwayJoinNode r2Node;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBaseTreeTrackerOneBetaHashTableOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    public TupleBaseTreeTrackerOneBetaHashTableOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TTJStatisticsInformation();
        }
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedTreeTrackerOneBetaHashTableOperator(this, context);
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

    protected Row lookUpH()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".lookUpH()"));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " is l null? " + isNull(l)));
            incrementTraceDepth();
        }
        if (l == null) {
            JoinValueContainerKey jav = extract(r1, true);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            l = hashTableH.get(jav);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("l = " + l));
            }
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
        }
        else if (iL.hasNext()) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("iL.hasNext() = true"));
            }
            currRowPointedbyIL = iL.next();
            Row ret = join(r1, currRowPointedbyIL);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("return " + ret));
                decrementTraceDepth();
            }
            return ret;
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return null"));
            decrementTraceDepth();
        }
        return null;
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            requireNonNull(info, "info is null");
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        Context context = (Context) info;
        if (context.getGetNewR1()) {
            currRowPointedbyIL.setIsGood(true);
            return r1Operator.passContext(info);
        }
        else {
            MultiwayJoinNode currNode = context.getJoinNode();
            MultiwayJoinOrderedGraph multiwayJoinOrderedGraph = planBuildContext.getOrderedGraph();
            if (multiwayJoinOrderedGraph.getParent().get(currNode).get(0).equals(r2Node)) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("%s is the parent of %s in G", r2Node, currNode)));
                }
                JoinValueContainerKey jav = extract(currRowPointedbyIL, false);
                if (!currRowPointedbyIL.getIsGood()) {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("to be removed from H: " + currRowPointedbyIL));
                    }
                    iL.remove();
                    if (Switches.STATS) {
                        statisticsInformation.incrementNumberOfNoGoodTuples();
                    }
                    List<Row> l = hashTableH.get(jav);
                    if (l.isEmpty()) {
                        hashTableH.remove(jav);
                    }
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
                r1 = r1Operator.passContext(info);
                if (Switches.DEBUG) {
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

    protected Row getR1(boolean getNewR1)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getR1(" + getNewR1 + ")"));
            incrementTraceDepth();
        }
        if (getNewR1) {
            if (l != null) {
                if (!currRowPointedbyIL.getIsGood()) {
                    passContext(new Context(getNewR1));
                }
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
            r1 = r1Operator.getNext();
            if (Switches.DEBUG) {
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
            r2 = lookUpH();
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
            r1 = r1Operator.passContext(context);
            if (Switches.DEBUG) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
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

    public static class Context
            implements OperatorInformation
    {
        private MultiwayJoinNode joinNode;
        private boolean getNewR1;

        public Context(MultiwayJoinNode node)
        {
            requireNonNull(node, "node is null");
            joinNode = node;
        }

        public Context(boolean getNewR1)
        {
            this.getNewR1 = getNewR1;
        }

        @Override
        public String toString()
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("(");
            if (joinNode != null) {
                stringBuilder.append(joinNode.getSchemaTableName().toString())
                        .append(",");
            }
            stringBuilder.append(getGetNewR1())
                    .append(")");
            return stringBuilder.toString();
        }

        public MultiwayJoinNode getJoinNode()
        {
            return joinNode;
        }

        public boolean getGetNewR1()
        {
            return getNewR1;
        }
    }

    @Override
    public void initializeContextObject()
    {
        context = r2Operator.getChildMultiwayJoinNode() == null ?
                new Context(r2Operator.getMultiwayJoinNode()) :
                new Context(r2Operator.getChildMultiwayJoinNode());
        r2Node = r2Operator.getChildMultiwayJoinNode() == null ?
                r2Operator.getMultiwayJoinNode() :
                r2Operator.getChildMultiwayJoinNode();
    }

    @Override
    public MultiwayJoinNode getMultiwayJoinNode()
    {
        return multiwayJoinNode;
    }

    @Override
    public void setMultiwayJoinNode(MultiwayJoinNode node)
    {
        this.multiwayJoinNode = node;
    }

    @Override
    public SchemaTableName getSchemaTableName()
    {
        if (multiwayJoinNode != null) {
            return multiwayJoinNode.getSchemaTableName();
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
