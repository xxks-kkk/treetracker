package org.zhu45.treetracker.relational.operator.noGoodList;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.OperatorInformation;
import org.zhu45.treetracker.relational.operator.TTJStatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanStatisticsInformation;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.openjdk.jol.info.GraphLayout.parseInstance;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap.ConstructNodeId2JoinIdx.checkNodeId2JoinIdxIfQuerySatisfiesRequirement;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap.constructNodeId2JoinIdx;

/**
 * Just like SingleValueJavNoGoodListMap but work with IntRow, which enables further simplification.
 */
public class SingleValueJavIntRowNoGoodListMap
        extends AbstractNoGoodListMap
{
    // key: nodeId, val: joinIdx for the current row, i.e., current_row[joinIdx]
    // is the jav between R_k and the relation identified by nodeId
    Int2IntOpenHashMap nodeId2JoinIdx;
    // key: joinIdx, val: no-good list consists of javs, which are integers
    // Note that if have a key clash, i.e., joinIdx from two different relations
    // are the same (e.g., R_k is title(movie_id) and it has two children
    // cast_info(movie_id) and movie_keyword(movie_id)). That is okay because
    // joinIdx is on the rows of R_k.
    Int2ObjectOpenHashMap<IntOpenHashSet> noGoodListMap;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(SingleValueJavIntRowNoGoodListMap.class);
        }
    }

    private SingleValueJavIntRowNoGoodListMap(TableNode outerTable, PlanBuildContext planBuildContext)
    {
        this.planBuildContext = planBuildContext;
        this.associateSchemaTableName = outerTable.getSchemaTableName();
        this.traceDepth = outerTable.getOperator().getOperatorTraceDepth() + 1;
        this.nodeId2JoinIdx = constructNodeId2JoinIdx(outerTable, planBuildContext);
        this.noGoodListMap = constructNoGoodListMap(nodeId2JoinIdx);
        if (Switches.STATS) {
            this.statisticsInformation = new TupleBasedTableScanStatisticsInformation();
            statisticsInformation.setNoGoodListMapKeyNum(nodeId2JoinIdx.size());
        }
    }

    @Override
    public boolean isGood(Row row)
    {
        if (Switches.STATS) {
            noGoodListProbingTimeMarker = System.nanoTime();
        }
        if (!noGoodListMapAddedValue) {
            if (Switches.STATS) {
                noGoodListProbingTime += (System.nanoTime() - noGoodListProbingTimeMarker);
            }
            return true;
        }
        int[] vals = ((IntRow) row).getIntVals();
        for (Int2ObjectMap.Entry<IntOpenHashSet> entry : noGoodListMap.int2ObjectEntrySet()) {
            if (entry.getValue().contains(vals[entry.getIntKey()])) {
                if (Switches.STATS) {
                    noGoodListProbingTime += (System.nanoTime() - noGoodListProbingTimeMarker);
                    statisticsInformation.incrementNumberOfNoGoodTuplesFiltered();
                }
                return false;
            }
        }
        if (Switches.STATS) {
            noGoodListProbingTime += (System.nanoTime() - noGoodListProbingTimeMarker);
        }
        return true;
    }

    @Override
    public void updateNoGoodListMap(Row row, OperatorInformation info)
    {
        if (Switches.STATS) {
            noGoodListConstructTimeMarker = System.nanoTime();
        }
        TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.Context context = (TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.Context) info;
        int[] vals = ((IntRow) row).getIntVals();
        int nodeId = context.getJoinNode().getNodeId();
        int joinIdx = nodeId2JoinIdx.get(nodeId);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + row.getVals().get(joinIdx)));
        }
        noGoodListMap.get(joinIdx).add(vals[joinIdx]);
        noGoodListMapAddedValue = true;
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("nodeId2JoinIdx: " + nodeId2JoinIdx));
            traceLogger.trace(formatTraceMessage("noGoodListMap: " + noGoodListMap));
        }
        if (Switches.STATS) {
            long currentTime = System.nanoTime();
            noGoodListConstructTime += (currentTime - noGoodListConstructTimeMarker);
            statisticsInformation.updateBuildNoGoodListTime(currentTime - context.getPassContextInitiationTime());
            ((TTJStatisticsInformation) statisticsInformation).incrementNodeToNoGoodTuples(context.getJoinNode());
            statisticsInformation.incrementNumberOfNoGoodTuples();
        }
    }

    @Override
    public void updateNoGoodListMap(Row row, int id)
    {
        int[] vals = ((IntRow) row).getIntVals();
        int joinIdx = nodeId2JoinIdx.get(id);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + row.getVals().get(joinIdx)));
        }
        noGoodListMap.get(joinIdx).add(vals[joinIdx]);
        noGoodListMapAddedValue = true;
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("nodeId2JoinIdx: " + nodeId2JoinIdx));
            traceLogger.trace(formatTraceMessage("noGoodListMap: " + noGoodListMap));
        }
    }

    @Override
    public String generateNoGoodListMapRepresentation()
    {
        List<MultiwayJoinNode> nodes = planBuildContext.getOrderedGraph().getTraversalList();
        List<Integer> nodeIds = nodes.stream().map(MultiwayJoinNode::getNodeId).collect(Collectors.toList());
        List<String> segments = new ArrayList<>();
        StringBuilder stringBuilder = new StringBuilder();
        if (nodeId2JoinIdx.size() != noGoodListMap.size()) {
            // there exists key clashes (multiple child relations point to the same joinIdx)
            // For example, if a node S(y,z) has three children H(z), B(z,w), and R(z,a),
            // all three children share attribute z and nodeId2JoinIdx has size 3: H -> 1, B -> 1, and R -> 1 and 1 comes from
            // the index of z in S. noGoodListMap has size 1; the only entry corresponds to attribute z.
            stringBuilder.append("warning: multiple key clashes exist\n");
        }
        for (Int2IntMap.Entry entry : nodeId2JoinIdx.int2IntEntrySet()) {
            StringBuilder builder = new StringBuilder();
            int nodeIdIdx = nodeIds.indexOf(Integer.valueOf(entry.getIntKey()));
            SchemaTableName schemaTableName = nodes.get(nodeIdIdx).getSchemaTableName();
            builder.append(schemaTableName)
                    .append(":");
            int joinIdx = entry.getIntValue();
            List<String> attributes = planBuildContext.getJdbcClient().getAttributes(associateSchemaTableName);
            String noGoodAttribute = attributes.get(joinIdx);
            builder.append("(")
                    .append(noGoodAttribute)
                    .append(")")
                    .append(":");
            builder.append("(")
                    .append(noGoodListMap.get(joinIdx).size())
                    .append(")")
                    .append(":");
            if (noGoodListMap.get(joinIdx).size() > maxPrintNoGoodListSize) {
                builder.append("[...]");
            }
            else {
                builder.append(noGoodListMap.get(joinIdx));
            }
            segments.add(builder.toString());
        }
        stringBuilder.append("R_k:")
                .append(associateSchemaTableName)
                .append("|")
                .append(Joiner.on("\n").join(segments));
        return stringBuilder.toString();
    }

    @Override
    public int getNoGoodListSize()
    {
        int size = 0;
        for (IntSet noGoodList : noGoodListMap.values()) {
            size += noGoodList.size();
        }
        return size;
    }

    @Override
    public int size()
    {
        return noGoodListMap.size();
    }

    @Override
    public long sizeInBytes()
    {
        return parseInstance(noGoodListMap).totalSize();
    }

    public static Pair<Boolean, SingleValueJavIntRowNoGoodListMap> constructSingleValueJavIntRowNoGoodListMap(TableNode outerTable, PlanNode root, PlanBuildContext planBuildContext)
    {
        Map<Integer, List<Integer>> nodeId2JoinIdx = planBuildContext.getNodeId2FactTableJoinAttributeIdx(outerTable);
        boolean ifNodeId2JoinIdxSatisfiesRequirement = checkNodeId2JoinIdxIfQuerySatisfiesRequirement(root, Triple.of(outerTable, planBuildContext, nodeId2JoinIdx));
        if (!ifNodeId2JoinIdxSatisfiesRequirement) {
            return Pair.of(ifNodeId2JoinIdxSatisfiesRequirement, null);
        }
        return Pair.of(ifNodeId2JoinIdxSatisfiesRequirement, new SingleValueJavIntRowNoGoodListMap(outerTable, planBuildContext));
    }

    private static Int2ObjectOpenHashMap<IntOpenHashSet> constructNoGoodListMap(Int2IntOpenHashMap nodeId2JoinIdx)
    {
        Int2ObjectOpenHashMap<IntOpenHashSet> noGoodListMap = new Int2ObjectOpenHashMap<>();
        for (Int2IntMap.Entry entry : nodeId2JoinIdx.int2IntEntrySet()) {
            noGoodListMap.computeIfAbsent(entry.getIntValue(), k -> IntOpenHashSet.of());
        }
        return noGoodListMap;
    }
}
