package org.zhu45.treetracker.relational.operator.noGoodList;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.Value;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.JoinValueContainerKey;
import org.zhu45.treetracker.relational.operator.OperatorInformation;
import org.zhu45.treetracker.relational.operator.TTJStatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanStatisticsInformation;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static org.openjdk.jol.info.GraphLayout.parseInstance;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;

public class DefaultNoGoodListMap
        extends AbstractNoGoodListMap
{
    private final Map<Integer, Pair<List<Integer>, NoGoodList<Value>>> noGoodListMap;

    private DefaultNoGoodListMap(PlanBuildContext planBuildContext)
    {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(DefaultNoGoodListMap.class);
        }
        this.planBuildContext = planBuildContext;
        this.associateSchemaTableName = planBuildContext.getLeftMostPlanNodeOperator().getSchemaTableName();
        this.traceDepth = planBuildContext.getLeftMostPlanNodeOperator().getOperatorTraceDepth() + 1;
        this.noGoodListMap = constructNoGoodListMap(planBuildContext);
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
        List<RelationalValue> rowVals = row.getVals();
        for (Integer nodeId : noGoodListMap.keySet()) {
            List<Integer> factTableJoinAttributeIdx = noGoodListMap.get(nodeId).getLeft();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                List<String> rowAttributes = row.getAttributes();
                traceLogger.trace(formatTraceMessage("rowAttributes: " + rowAttributes));
                traceLogger.trace(formatTraceMessage("rowVals: " + rowVals));
            }
            List<RelationalValue> vals = new ObjectArrayList<>(factTableJoinAttributeIdx.size());
            for (int idx : factTableJoinAttributeIdx) {
                vals.add(rowVals.get(idx));
            }
            JoinValueContainerKey jav = new JoinValueContainerKey(new ArrayList<>(vals));
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessage("jav: " + jav));
            }
            NoGoodList<Value> noGoodList1 = noGoodListMap.get(nodeId).getRight();
            if (noGoodList1.contains(jav)) {
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
        Integer nodeId = Integer.valueOf(context.getJoinNode().getNodeId());
        List<Integer> factTableJoinAttributeIdx = noGoodListMap.get(nodeId).getLeft();
        List<RelationalValue> rowVals = row.getVals();
        JoinValueContainerKey jav = new JoinValueContainerKey(
                factTableJoinAttributeIdx.stream().map(rowVals::get).collect(Collectors.toList()));
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
    public String generateNoGoodListMapRepresentation()
    {
        List<MultiwayJoinNode> nodes = planBuildContext.getOrderedGraph().getTraversalList();
        List<Integer> nodeIds = nodes.stream().map(MultiwayJoinNode::getNodeId).collect(Collectors.toList());
        List<String> segments = new ArrayList<>();
        for (Integer nodeId : noGoodListMap.keySet()) {
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
        for (Pair<List<Integer>, NoGoodList<Value>> val : noGoodListMap.values()) {
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

    public static DefaultNoGoodListMap constructDefaultNoGoodListMap(PlanBuildContext planBuildContext)
    {
        return new DefaultNoGoodListMap(planBuildContext);
    }

    private static Map<Integer, Pair<List<Integer>, NoGoodList<Value>>> constructNoGoodListMap(PlanBuildContext context)
    {
        Map<Integer, Pair<List<Integer>, NoGoodList<Value>>> noGoodListMap = new Int2ObjectOpenHashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : context.getNodeId2FactTableJoinAttributeIdx().entrySet()) {
            if (Switches.DEBUG) {
                checkArgument(!entry.getValue().isEmpty(), "factTableJoinAttributeIdx is empty");
            }
            Class<? extends NoGoodList> noGoodListClazz = context.getNoGoodList().getClass();
            try {
                Method method = noGoodListClazz.getMethod("create");
                NoGoodList<Value> noGoodList1 = (NoGoodList) method.invoke(noGoodListClazz);
                noGoodListMap.put(entry.getKey(), Pair.of(entry.getValue(), noGoodList1));
            }
            catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
            }
        }
        return noGoodListMap;
    }
}
