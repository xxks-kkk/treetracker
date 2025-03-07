package org.zhu45.treetracker.relational.operator.noGoodList;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerIntKey;
import org.zhu45.treetracker.relational.operator.OperatorInformation;
import org.zhu45.treetracker.relational.operator.TTJStatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanStatisticsInformation;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.openjdk.jol.info.GraphLayout.parseInstance;

/**
 * IntRow version of DefaultNoGoodListMap
 */
public class DefaultIntRowNoGoodListMap
        extends AbstractNoGoodListMap
{
    // key: nodeId, val: Pair<an array of integer, no-good list consists of javs>,
    // - an array of integer is join idx for the current row, i.e., suppose val is [0,2].
    //   Then, (current_row[0],current_row[2]) altogether forms the jav between R_k and the relation identified by nodeId
    // - no-good list consists of javs
    Int2ObjectMap<Pair<IntList, ObjectOpenHashSet<JoinValueContainerIntKey>>> noGoodListMap;

    private DefaultIntRowNoGoodListMap(TableNode outerTable, PlanBuildContext planBuildContext)
    {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(DefaultNoGoodListMap.class);
        }
        this.planBuildContext = planBuildContext;
        this.associateSchemaTableName = outerTable.getSchemaTableName();
        this.traceDepth = outerTable.getOperator().getOperatorTraceDepth() + 1;
        constructNoGoodListMap(outerTable, planBuildContext);
        if (Switches.STATS) {
            this.statisticsInformation = new TupleBasedTableScanStatisticsInformation();
        }
    }

    /**
     * Check if the given row is good to return
     */
    @Override
    public boolean isGood(Row row)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage(associateSchemaTableName + ".isGood(" + row + ")"));
        }
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
        for (int nodeId : noGoodListMap.keySet()) {
            IntList factTableJoinAttributeIdx = noGoodListMap.get(nodeId).getLeft();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                List<String> rowAttributes = row.getAttributes();
                traceLogger.trace(formatTraceMessage("rowAttributes: " + rowAttributes));
                traceLogger.trace(formatTraceMessage("rowVals: " + vals));
            }
            int[] javVal = new int[factTableJoinAttributeIdx.size()];
            for (int i = 0; i < factTableJoinAttributeIdx.size(); i++) {
                javVal[i] = vals[factTableJoinAttributeIdx.getInt(i)];
            }
            JoinValueContainerIntKey jav = new JoinValueContainerIntKey(javVal);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            if (noGoodListMap.get(nodeId).getRight().contains(jav)) {
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessage("return false"));
                }
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
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("return true"));
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
        int nodeId = context.getJoinNode().getNodeId();
        IntList factTableJoinAttributeIdx = noGoodListMap.get(nodeId).getLeft();
        int[] vals = ((IntRow) row).getIntVals();
        int[] javVal = new int[factTableJoinAttributeIdx.size()];
        for (int i = 0; i < factTableJoinAttributeIdx.size(); i++) {
            javVal[i] = vals[factTableJoinAttributeIdx.getInt(i)];
        }
        JoinValueContainerIntKey jav = new JoinValueContainerIntKey(javVal);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + jav));
        }
        noGoodListMap.get(nodeId).getRight().add(jav);
        noGoodListMapAddedValue = true;
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
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
        IntList factTableJoinAttributeIdx = noGoodListMap.get(id).getLeft();
        int[] javVal = new int[factTableJoinAttributeIdx.size()];
        for (int i = 0; i < factTableJoinAttributeIdx.size(); i++) {
            javVal[i] = vals[factTableJoinAttributeIdx.getInt(i)];
        }
        JoinValueContainerIntKey jav = new JoinValueContainerIntKey(javVal);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + jav));
        }
        noGoodListMap.get(id).getRight().add(jav);
        noGoodListMapAddedValue = true;
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("noGoodListMap: " + noGoodListMap));
        }
    }

    @Override
    public String generateNoGoodListMapRepresentation()
    {
        List<MultiwayJoinNode> nodes = planBuildContext.getOrderedGraph().getTraversalList();
        List<Integer> nodeIds = nodes.stream().map(MultiwayJoinNode::getNodeId).collect(Collectors.toList());
        List<String> segments = new ArrayList<>();
        for (int nodeId : noGoodListMap.keySet()) {
            StringBuilder builder = new StringBuilder();
            int nodeIdIdx = nodeIds.indexOf(nodeId);
            SchemaTableName schemaTableName = nodes.get(nodeIdIdx).getSchemaTableName();
            builder.append(schemaTableName)
                    .append(":");
            List<String> attributes = planBuildContext.getJdbcClient().getAttributes(associateSchemaTableName);
            List<String> noGoodAttributes = noGoodListMap.get(nodeId).getLeft().stream().map(attributes::get).collect(Collectors.toList());
            builder.append("(")
                    .append(Joiner.on(",").join(noGoodAttributes))
                    .append(")")
                    .append(":");
            builder.append("(")
                    .append(noGoodListMap.get(nodeId).getRight().size())
                    .append(")")
                    .append(":");
            if (noGoodListMap.get(nodeId).getRight().size() > maxPrintNoGoodListSize) {
                builder.append("[...]");
            }
            else {
                builder.append(noGoodListMap.get(nodeId).getRight());
            }
            segments.add(builder.toString());
        }
        return "R_k:" + associateSchemaTableName + "|"
                + Joiner.on("\n").join(segments);
    }

    @Override
    public int getNoGoodListSize()
    {
        int size = 0;
        for (Pair<IntList, ObjectOpenHashSet<JoinValueContainerIntKey>> val : noGoodListMap.values()) {
            size += val.getRight().size();
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

    public static DefaultIntRowNoGoodListMap constructDefaultIntRowNoGoodListMap(TableNode outerTable, PlanBuildContext planBuildContext)
    {
        return new DefaultIntRowNoGoodListMap(outerTable, planBuildContext);
    }

    private void constructNoGoodListMap(TableNode outerTable, PlanBuildContext context)
    {
        noGoodListMap = new Int2ObjectOpenHashMap<>();
        Map<Integer, List<Integer>> nodeId2JoinIdx = context.getNodeId2FactTableJoinAttributeIdx(outerTable);
        for (Map.Entry<Integer, List<Integer>> entry : nodeId2JoinIdx.entrySet()) {
            checkArgument(!entry.getValue().isEmpty(), "factTableJoinAttributeIdx is empty");
            noGoodListMap.put(entry.getKey().intValue(), Pair.of(new IntArrayList(entry.getValue()), new ObjectOpenHashSet<>()));
        }
    }
}
