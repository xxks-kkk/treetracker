package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerIntKey;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;

/**
 * A special case of TupleBasedHashJoinOperator where Row consists of all integers.
 */
public class TupleBasedIntHashJoinOperator
        extends TupleBasedHashJoinOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedIntHashJoinOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected Iterator<IntRow> iL;
    protected IntRow currRowPointedbyIL;
    protected List<IntRow> l;
    protected Map<JoinValueContainerIntKey, List<IntRow>> hashTableH;

    private long buildHashTableTimeMarker;

    public TupleBasedIntHashJoinOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedHashJoinStatisticsInformation();
            stopWatch = new StopWatch();
        }
    }

    @Override
    public void open()
    {
        hashJoinOpen();
    }

    protected void hashJoinOpen()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".open()"));
            incrementTraceDepth();
        }
        r1Operator.open();
        r2Operator.open();

        if (Switches.STATS) {
            buildHashTableTimeMarker = System.nanoTime();
        }
        hashTableH = UnifiedMap.newMap((int) ((float) operatorAssociatedRelationSize / 0.75F + 1.0F));
        int i = 0;
        while (true) {
            r2 = r2Operator.getNext();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".r2 = " + r2));
            }
            if (r2 == null) {
                r2Operator.close();
                break;
            }
            JoinValueContainerIntKey jav = extract((IntRow) r2, false);
            if (!hashTableH.containsKey(jav)) {
                hashTableH.put(jav, new LinkedList<>());
            }
            hashTableH.get(jav).add((IntRow) r2);
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                i++;
                if (i % 1000 == 0) {
                    traceLogger.debug("number of rows added: " + i);
                }
            }
        }
        if (Switches.STATS) {
            statisticsInformation.setHashTableBuildTime(System.nanoTime() - buildHashTableTimeMarker);
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".H = " + hashTableH));
            decrementTraceDepth();
        }
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("finish hash table construction in " + r2Operator.getTraceOperatorName());
        }
    }

    @Override
    public void close()
    {
        iL = null;
        currRowPointedbyIL = null;
        l = null;
        hashTableH = null;

        if (Switches.STATS) {
            statisticsInformation.setHashTableInitialAllocationCapacity((int) ((float) operatorAssociatedRelationSize / 0.75F + 1.0F));
            updateStatisticsInformatAtClose();
        }

        r1Operator.close();
        r2Operator.close();
    }

    protected void updateStatisticsInformatAtClose()
    {
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
                if (Switches.STATS) {
                    stopWatch.reset();
                    stopWatch.start();
                }
                IntRow ret = join((IntRow) r1, currRowPointedbyIL);
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

        while (true) {
            r1 = r1Operator.getNext();
            if (Switches.STATS) {
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            if (r1 == null) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return " + null));
                    decrementTraceDepth();
                }
                return null;
            }
            if (Switches.STATS) {
                stopWatch.reset();
                stopWatch.start();
            }
            r2 = lookUpH();
            if (Switches.STATS) {
                stopWatch.stop();
                statisticsInformation.updateJoinTime(stopWatch.getNanoTime());
            }
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
        }
    }

    private IntRow lookUpH()
    {
        long probeHashTableTime;
        if (Switches.STATS) {
            probeHashTableTime = System.nanoTime();
            statisticsInformation.incrementNumberOfHashTableProbe();
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".lookUpH()"));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".l = " + l));
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + " is l null? " + isNull(l)));
            incrementTraceDepth();
        }
        JoinValueContainerIntKey jav = extract((IntRow) r1, true);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + jav));
        }
        l = hashTableH.get(jav);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("l = " + l));
        }
        if (Switches.STATS) {
            statisticsInformation.updateProbeHashTableTime(System.nanoTime() - probeHashTableTime);
        }
        if (l != null) {
            iL = l.iterator();
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
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return null"));
            decrementTraceDepth();
        }
        return null;
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedHashJoinOperator(this, context);
    }
}
