package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Objects.isNull;

public class TupleBasedHashJoinOperator
        extends TupleBasedJoinOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedHashJoinOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected Iterator<Row> iL;
    protected Row currRowPointedbyIL;
    protected List<Row> l;
    protected Map<JoinValueContainerKey, List<Row>> hashTableH;

    private long buildHashTableTimeMarker;

    public TupleBasedHashJoinOperator()
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
            JoinValueContainerKey jav = extract(r2, false);
            if (!hashTableH.containsKey(jav)) {
                hashTableH.put(jav, new LinkedList<>());
            }
            hashTableH.get(jav).add(r2);
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
        if (Switches.STATS) {
            if (hashTableH != null) {
                statisticsInformation.updateHashTableSizeAfterEvaluation(getHashTableSizeAfterEvaluation());
            }
        }
        hashTableH = null;

        if (Switches.STATS) {
            statisticsInformation.setHashTableInitialAllocationCapacity((int) ((float) operatorAssociatedRelationSize / 0.75F + 1.0F));
            updateStatisticsInformatAtClose();
        }

        r1Operator.close();
        r2Operator.close();
    }

    private long getHashTableSizeAfterEvaluation()
    {
        long size = 0;
        for (var val : hashTableH.values()) {
            size += val.size();
        }
        return size;
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

    private Row lookUpH()
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
        JoinValueContainerKey jav = extract(r1, true);
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
                Row ret = join(r1, currRowPointedbyIL);
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

    public Map<JoinValueContainerKey, List<Row>> getHashTableH()
    {
        return hashTableH;
    }
}
