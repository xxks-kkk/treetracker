package org.zhu45.treetracker.relational.operator;

import de.renebergelt.test.Switches;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treetracker.common.BlockedBloom;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;

import java.util.ArrayList;
import java.util.List;

/**
 * LIP Table Scan operator for SSB
 */
public class TupleBasedSSBLIPTableScanOperator
        extends TupleBasedLIPTableScanOperator
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TupleBasedSSBLIPTableScanOperator.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    // key: nodeId, val: joinIdx for the current row, i.e., current_row[joinIdx]
    // is the jav between R_k and the relation identified by nodeId
    Int2IntOpenHashMap nodeId2JoinIdx = new Int2IntOpenHashMap();
    // key: joinIdx, val: BloomFilter
    Int2ObjectOpenHashMap<BlockedBloom> bloomFilterMap = new Int2ObjectOpenHashMap<>();

    public TupleBasedSSBLIPTableScanOperator()
    {
        if (Switches.STATS) {
            statisticsInformation = new TupleBasedLIPTableScanStatisticsInformation();
        }
    }

    @Override
    protected boolean isGood(Row row)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(getTraceOperatorName() + ".isGood(" + row + ")"));
            incrementTraceDepth();
        }
        for (int nodeId : nodeId2JoinIdx.keySet()) {
            int factTableJoinAttributeIdx = nodeId2JoinIdx.get(nodeId);
            int[] vals = ((IntRow) row).getIntVals();
            List<RelationalValue> rowVals = new ArrayList<>();
            rowVals.add(IntegerValue.of(vals[factTableJoinAttributeIdx]));
            JoinValueContainerKey jav = new JoinValueContainerKey(rowVals);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            BlockedBloom bloomFilter = bloomFilterMap.get(factTableJoinAttributeIdx);
            if (!bloomFilter.mayContain(jav.hashCode())) {
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
        TupleBasedSSBLIPHashJoinOperator.Context context = (TupleBasedSSBLIPHashJoinOperator.Context) info;
        int factTableJoinAttributeIdx = context.getFactTableJoinAttributeIdx();
        BlockedBloom bloomFilter = context.getBloomFilter();
        int nodeId = context.getNodeId();
        nodeId2JoinIdx.put(nodeId, factTableJoinAttributeIdx);
        bloomFilterMap.put(factTableJoinAttributeIdx, bloomFilter);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("factTableJoinAttributeIdx: " + factTableJoinAttributeIdx));
            traceLogger.trace(formatTraceMessage("bloomFilterMap: " + bloomFilterMap));
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            decrementTraceDepth();
        }
    }
}
