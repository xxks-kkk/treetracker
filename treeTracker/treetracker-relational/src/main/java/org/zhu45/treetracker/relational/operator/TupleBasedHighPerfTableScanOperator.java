package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodListMap;

import java.util.NoSuchElementException;

import static java.util.Objects.requireNonNull;

/**
 * Enhancements compared to TupleBasedTableScanOperator:
 * 1. NoGoodList contains jav instead of row
 */
public class TupleBasedHighPerfTableScanOperator
        extends TupleBasedTableScanOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedHighPerfTableScanOperator.class);
        }
        else {
            traceLogger = null;
        }
    }

    NoGoodListMap noGoodListMap;

    @Override
    public void close()
    {
        if (Switches.STATS && !statisticsUpdatedInClose) {
            statisticsInformation.setFetchingTuplesTime(fetchingTuplesTime);
            statisticsInformation.setNumberOfTuples(operatorAssociatedRelationSize);
            statisticsInformation.setPredicateEvaluationTime(predicateEvaluationTime);
            statisticsInformation.setRecordTupleSourceClazzName(recordTupleSourceProvider.getClass().getCanonicalName());
            noGoodListMap.updateStatisticsInformation();
            statisticsUpdatedInClose = true;
        }
        finish();
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }

        try {
            while (true) {
                if (Switches.STATS) {
                    fetchingTupleTimeMarker = System.nanoTime();
                }
                Row tmp = getNextRow();
                if (Switches.STATS) {
                    fetchingTuplesTime += (System.nanoTime() - fetchingTupleTimeMarker);
                }
                if (noGoodListMap.isGood(tmp)) {
                    pVal = tmp;
                    break;
                }
            }
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("pVal: " + pVal));
                traceLogger.trace(formatTraceMessage("return: " + pVal));
                decrementTraceDepth();
            }
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s in %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1,
                            getTraceOperatorName())));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            return pVal;
        }
        catch (NoSuchElementException e) {
            pVal = null;
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("pVal: " + pVal));
                traceLogger.trace(formatTraceMessage("return: " + pVal));
                decrementTraceDepth();
            }
            if (Switches.STATS) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage(String.format("incrementNumberOfR1Assignment from %s to %s in %s",
                            statisticsInformation.getNumberOfR1Assignments(),
                            statisticsInformation.getNumberOfR1Assignments() + 1,
                            getTraceOperatorName())));
                }
                statisticsInformation.incrementNumberOfR1Assignments();
            }
            return pVal;
        }
    }

    @Override
    public Row passContext(OperatorInformation info)
    {
        if (Switches.STATS) {
            statisticsInformation.incrementNumberOfPassContextCalls();
            statisticsInformation.incrementNumberOfDanglingTuples();
        }
        if (Switches.DEBUG) {
            requireNonNull(info, "info is null");
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
                incrementTraceDepth();
            }
        }
        noGoodListMap.updateNoGoodListMap(pVal, info);
        Row tmp = getNext();
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return: " + tmp));
            decrementTraceDepth();
        }
        return tmp;
    }

    @Override
    public void setNoGoodListMap(NoGoodListMap noGoodListMap)
    {
        this.noGoodListMap = noGoodListMap;
    }

    @Override
    public NoGoodListMap getNoGoodListMap()
    {
        return this.noGoodListMap;
    }
}
