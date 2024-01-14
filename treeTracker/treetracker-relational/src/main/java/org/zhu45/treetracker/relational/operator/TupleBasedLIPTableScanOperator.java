package org.zhu45.treetracker.relational.operator;

import com.google.common.hash.BloomFilter;
import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

/**
 * Used by Lookahead Information Passing (LIP)
 */
public class TupleBasedLIPTableScanOperator
        extends TupleBasedTableScanOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedLIPTableScanOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private long bloomFiltersProbingTimeMarker;
    private long bloomFiltersProbingTime;

    private Map<SchemaTableName, Pair<List<Integer>, BloomFilter>> bloomFilterMap = UnifiedMap.newMap();

    public TupleBasedLIPTableScanOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedLIPTableScanStatisticsInformation();
        }
    }

    @Override
    public <C> void accept(OperatorVisitor<C> visitor, C context)
    {
        visitor.visitTupleBasedLIPTableScanOperator(this, context);
    }

    @Override
    public Row getNext()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".getNext()"));
            incrementTraceDepth();
        }
        if (Switches.STATS) {
            statisticsInformation.setNumberOfBloomFiltersRegistered(bloomFilterMap.keySet().size());
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("bloomFilterMap: " + bloomFilterMap));
        }
        try {
            while (true) {
                Row tmp = source.getNextRow();
                if (Switches.STATS) {
                    bloomFiltersProbingTimeMarker = System.nanoTime();
                }
                if (isGood(tmp)) {
                    if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessage("return: " + tmp));
                        decrementTraceDepth();
                    }
                    if (Switches.STATS) {
                        bloomFiltersProbingTime += (System.nanoTime() - bloomFiltersProbingTimeMarker);
                    }
                    return tmp;
                }
                else {
                    if (Switches.STATS) {
                        bloomFiltersProbingTime += (System.nanoTime() - bloomFiltersProbingTimeMarker);
                        statisticsInformation.incrementNumberOfTuplesFilterOutByBloomFilters();
                    }
                }
            }
        }
        catch (NoSuchElementException e) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("return: null"));
                decrementTraceDepth();
            }
            if (Switches.STATS) {
                statisticsInformation.setBloomFiltersProbingTime(bloomFiltersProbingTime);
            }
            return null;
        }
    }

    @Override
    public void open()
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".open()"));
        }
        reset();
    }

    /**
     * Check if the given row can pass all Bloom filters
     */
    protected boolean isGood(Row row)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".isGood(" + row + ")"));
            incrementTraceDepth();
        }
        for (SchemaTableName schemaTableName : bloomFilterMap.keySet()) {
            List<Integer> factTableJoinAttributeIdx = bloomFilterMap.get(schemaTableName).getLeft();
            List<RelationalValue> rowVals = row.getVals();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                List<String> rowAttributes = row.getAttributes();
                traceLogger.trace(formatTraceMessage("rowAttributes: " + rowAttributes));
                traceLogger.trace(formatTraceMessage("rowVals: " + rowVals));
            }
            JoinValueContainerKey jav = new JoinValueContainerKey(
                    factTableJoinAttributeIdx.stream().map(rowVals::get).collect(Collectors.toList()));
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            BloomFilter bloomFilter = bloomFilterMap.get(schemaTableName).getRight();
            if (!bloomFilter.mightContain(jav)) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return false"));
                    decrementTraceDepth();
                }
                return false;
            }
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return true"));
            decrementTraceDepth();
        }
        return true;
    }

    @Override
    public ObjectRow passContext(OperatorInformation info)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".passContext(" + info + ")"));
            incrementTraceDepth();
        }
        registerBloomFilter(info);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            decrementTraceDepth();
        }
        return null;
    }

    private void registerBloomFilter(OperatorInformation info)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getOperatorName() + ".registerBloomFilter()"));
            incrementTraceDepth();
        }
        TupleBasedLIPHashJoinOperator.Context context = (TupleBasedLIPHashJoinOperator.Context) info;
        List<Integer> factTableJoinAttributeIdx = context.getFactTableJoinAttributeIdx();
        BloomFilter bloomFilter = context.getBloomFilter();
        SchemaTableName schemaTableName = context.getSchemaTableName();
        bloomFilterMap.put(schemaTableName, Pair.of(factTableJoinAttributeIdx, bloomFilter));
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("factTableJoinAttributeIdx: " + factTableJoinAttributeIdx));
            traceLogger.trace(formatTraceMessage("bloomFilterMap: " + bloomFilterMap));
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            decrementTraceDepth();
        }
    }
}
