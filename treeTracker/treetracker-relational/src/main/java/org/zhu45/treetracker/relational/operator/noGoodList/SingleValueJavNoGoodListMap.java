package org.zhu45.treetracker.relational.operator.noGoodList;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.OperatorInformation;
import org.zhu45.treetracker.relational.operator.TTJStatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanStatisticsInformation;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanVisitor;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.openjdk.jol.info.GraphLayout.parseInstance;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.relational.operator.noGoodList.SingleValueJavNoGoodListMap.ConstructNodeId2JoinIdx.checkNodeId2JoinIdxIfQuerySatisfiesRequirement;

/**
 * This is a semantic optimized noGoodListMap implementation that
 * targets at any query whose R_k has the following characteristics:
 * - |ja(R_k, R_i)| = 1 for any child of R_k in join tree (they only share
 * one join attribute)
 * - the type of the one join attribute is integer
 * <p>
 * As an example, consider Q7aOptJoinTreeOptOrdering, R_k is cast_info and it has
 * two children in the join tree: person_info and movie_link. The no-good list
 * based on DefaultNoGoodListMap has the following content:
 * - person_info (person_id) (4042899)
 * - movie_link (movie_id) (7356)
 * That is, the jav between cast_info and person_info contains is person_id, which
 * is jav with only one value and has type integer. The corresponding noGoodList contains 4042899 javs,
 * which effectively are singe values, i.e., integers.
 * <p>
 * This implementation targets at queries with above characteristics and optimize over DefaultNoGoodListMap
 * in the following two ways:
 * 1. Simplify internal data structure. The DefaultNoGoodListMap has Map<Integer, Pair<List<Integer>, NoGoodList>> noGoodListMap.
 * We get rid of List<Integer>. In addition, we also remove Pair and replace it with two standalone maps. Implicitly, that requires
 * all javs for each child relation of R_k are single values.
 * 2. Simplify data stored in NoGoodList. In DefaultNoGoodListMap, we construct JoinValueContainerKey to be put into NoGoodList,
 * which based on profiling, impose a significant overhead. In this implementation, we don't construct JoinValueContainerKey at all
 * and directly put IntegerValue into the PlainIntegerNoGoodList.
 */
public class SingleValueJavNoGoodListMap
        extends AbstractNoGoodListMap
{
    // key: nodeId, val: joinIdx for the current row, i.e., current_row[joinIdx]
    // is the jav between R_k and the relation identified by nodeId
    Int2IntOpenHashMap nodeId2JoinIdx;
    // key: joinIdx, val: no-good list consists of javs, which are integers
    Int2ObjectOpenHashMap<NoGoodList<IntegerValue>> noGoodListMap;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(SingleValueJavNoGoodListMap.class);
        }
    }

    private SingleValueJavNoGoodListMap(PlanBuildContext planBuildContext)
    {
        this.planBuildContext = planBuildContext;
        this.associateSchemaTableName = planBuildContext.getLeftMostPlanNodeOperator().getSchemaTableName();
        this.traceDepth = planBuildContext.getLeftMostPlanNodeOperator().getOperatorTraceDepth() + 1;
        this.nodeId2JoinIdx = constructNodeId2JoinIdx(planBuildContext);
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
        // For checking if a row is good, we only need to use noGoodListMap because we need to iterate through
        // all possible joinIdx anyway.
        List<RelationalValue> vals = row.getVals();
        for (Int2ObjectMap.Entry<NoGoodList<IntegerValue>> entry : noGoodListMap.int2ObjectEntrySet()) {
            if (entry.getValue().contains((IntegerValue) vals.get(entry.getIntKey()))) {
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
        int nodeId = context.getJoinNode().getNodeId();
        int joinIdx = nodeId2JoinIdx.get(nodeId);
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessage("jav: " + row.getVals().get(joinIdx)));
        }
        noGoodListMap.get(joinIdx).add((IntegerValue) row.getVals().get(joinIdx));
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
    public String generateNoGoodListMapRepresentation()
    {
        List<MultiwayJoinNode> nodes = planBuildContext.getOrderedGraph().getTraversalList();
        List<Integer> nodeIds = nodes.stream().map(MultiwayJoinNode::getNodeId).collect(Collectors.toList());
        List<String> segments = new ArrayList<>();
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
        return "R_k:" + associateSchemaTableName + "|"
                + Joiner.on("\n").join(segments);
    }

    @Override
    public int getNoGoodListSize()
    {
        int size = 0;
        for (NoGoodList<IntegerValue> noGoodList : noGoodListMap.values()) {
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

    public static Pair<Boolean, SingleValueJavNoGoodListMap> constructSingleValueJavNoGoodListMap(PlanNode root, PlanBuildContext planBuildContext)
    {
        boolean isNodeId2JoinIdxSatisfiesRequirement = checkNodeId2JoinIdxIfQuerySatisfiesRequirement(root, planBuildContext);
        if (!isNodeId2JoinIdxSatisfiesRequirement) {
            return Pair.of(isNodeId2JoinIdxSatisfiesRequirement, null);
        }
        return Pair.of(isNodeId2JoinIdxSatisfiesRequirement, new SingleValueJavNoGoodListMap(planBuildContext));
    }

    public static Int2IntOpenHashMap constructNodeId2JoinIdx(PlanBuildContext context)
    {
        Map<Integer, List<Integer>> nodeId2JoinIdx = context.getNodeId2FactTableJoinAttributeIdx();
        Int2IntOpenHashMap newNodeId2JoinIdx = new Int2IntOpenHashMap();
        for (Map.Entry<Integer, List<Integer>> entry : nodeId2JoinIdx.entrySet()) {
            newNodeId2JoinIdx.put(entry.getKey().intValue(), entry.getValue().get(0).intValue());
        }
        return newNodeId2JoinIdx;
    }

    private static Int2ObjectOpenHashMap<NoGoodList<IntegerValue>> constructNoGoodListMap(Int2IntOpenHashMap nodeId2JoinIdx)
    {
        Int2ObjectOpenHashMap<NoGoodList<IntegerValue>> noGoodListMap = new Int2ObjectOpenHashMap<>();
        for (Int2IntMap.Entry entry : nodeId2JoinIdx.int2IntEntrySet()) {
            noGoodListMap.computeIfAbsent(entry.getIntValue(), k -> PlainIntegerNoGoodList.create());
        }
        return noGoodListMap;
    }

    public static class ConstructNodeId2JoinIdx
            extends PlanVisitor<Boolean, PlanBuildContext>
    {
        public static boolean checkNodeId2JoinIdxIfQuerySatisfiesRequirement(PlanNode root, PlanBuildContext context)
        {
            return new ConstructNodeId2JoinIdx().visitPlan(root, context);
        }

        @Override
        public Boolean visitPlan(PlanNode node, PlanBuildContext context)
        {
            List<PlanNode> children = node.getSources();
            for (PlanNode child : children) {
                if (child != null) {
                    Boolean isQuerySatisfiesRequirement = visitPlan(child, context);
                    if (isQuerySatisfiesRequirement == Boolean.FALSE) {
                        return isQuerySatisfiesRequirement;
                    }
                }
            }
            return node.accept(this, context);
        }

        @Override
        public Boolean visitJoin(JoinNode node, PlanBuildContext context)
        {
            return Boolean.TRUE;
        }

        @Override
        public Boolean visitTable(TableNode node, PlanBuildContext context)
        {
            Operator operator = node.getOperator();
            int nodeId = operator.getMultiwayJoinNode().getNodeId();
            if (operator.getPlanBuildContext().getNodeId2FactTableJoinAttributeIdx().containsKey(nodeId)) {
                return checkFactTableJoinAttributeIdx(nodeId, context);
            }
            return Boolean.TRUE;
        }

        @Override
        public Boolean visitFullReducer(FullReducerNode node, PlanBuildContext context)
        {
            throw new UnsupportedOperationException();
        }

        private boolean checkFactTableJoinAttributeIdx(int nodeId, PlanBuildContext context)
        {
            List<Integer> factTableJoinAttributeIdx = context.getNodeId2FactTableJoinAttributeIdx().get(nodeId);

            Operator factTableOperator = context.getLeftMostPlanNodeOperator();
            List<Type> factTableTypes = context.getCatalogGroup().getTableCatalog(factTableOperator.getSchemaTableName()).getTypeList();
            return factTableJoinAttributeIdx.size() == 1 &&
                    factTableTypes.get(factTableJoinAttributeIdx.get(0)).equals(INTEGER);
        }
    }
}
