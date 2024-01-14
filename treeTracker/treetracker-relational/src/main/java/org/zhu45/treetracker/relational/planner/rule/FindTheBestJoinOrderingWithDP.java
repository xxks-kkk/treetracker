package org.zhu45.treetracker.relational.planner.rule;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import lombok.Getter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.catalog.CatalogGroup;
import org.zhu45.treetracker.relational.planner.cost.CardEstContext;
import org.zhu45.treetracker.relational.planner.cost.CardEstProvider;
import org.zhu45.treetracker.relational.planner.cost.CardEstReturn;
import org.zhu45.treetracker.relational.planner.cost.CardEstType;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeCostReturn;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.common.Utils.formatTraceMessageWithDepth;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;
import static org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP.DPTableEntryId.getDPTableEntryId;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.checkIfCrossProductsFreeOrderingExists;

/***
 * The rule finds the best join ordering using dynamic programming approach.
 * Our DP subjects to the following assumptions:
 * - left-deep plan is considered.
 * - We don't consider cross product.
 * - we focus on intermediate result size as our cost, i.e., logical cost.
 * <p>
 * We reference "Database Systems: The Complete Book" for our implementation.
 * <p>
 * This rule only works for non-TTJ join operators
 */
public class FindTheBestJoinOrderingWithDP
        extends BaseRule
{
    private static final Logger traceLogger;
    protected int traceDepth = 1;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(FindTheBestJoinOrderingWithDP.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    protected CardEstProvider costProvider;
    private RuleStatistics.Builder builder = RuleStatistics.builder(this.getClass());
    private boolean enableHJPhysicalHeuristics;

    public FindTheBestJoinOrderingWithDP(CardEstProvider costProvider)
    {
        checkArgument(costProvider.getCardEstType() == CardEstType.NONTTJ);
        this.costProvider = costProvider;
    }

    public FindTheBestJoinOrderingWithDP(CardEstProvider costProvider, FindTheBestJoinOrderingJoinTreeWithDP.RuleConfiguration config)
    {
        checkArgument(costProvider.getCardEstType() == CardEstType.NONTTJ);
        this.costProvider = costProvider;
        this.enableHJPhysicalHeuristics = config.isEnableHJPhysicalHeuristics();
    }

    protected FindTheBestJoinOrderingWithDP()
    {
    }

    @Override
    public Plan applyToLogicalPlan(Plan plan, PlanBuildContext context)
    {
        return plan;
    }

    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        ruleContext = context;
        List<SchemaTableName> schemaTableNameList = getSchemaTableNames(node);
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("schemaTableNameList for DPTable: " + schemaTableNameList);
        }
        DPTable dpTable = new DPTable(schemaTableNameList, context.getPlanBuildContext().getCatalogGroup());
        DPTableEntry tableEntry = dpTable.getBestPlan();
        JoinOrdering optimalJoinOrdering = new JoinOrdering(tableEntry.planAssociatedWithOptCost);
        builder = builder.cost(tableEntry.optCost)
                .dpTable(dpTable)
                .optimalJoinOrdering(optimalJoinOrdering);
        if (context.getPlanBuildContext().getOrderedGraph() != null) {
            if (!optimalJoinOrdering.getSchemaTableNameList().equals(context.getPlanBuildContext()
                    .getOrderedGraph()
                    .getTraversalList()
                    .stream()
                    .map(MultiwayJoinNode::getSchemaTableName)
                    .collect(Collectors.toList()))) {
                // TODO: ideally, we should reverse engineer a join tree that is consistent with the optimal join ordering
                // because otherwise, it would cause unexpected scenario where we pass in an ordered graph during the query
                // construction and then the ordered graph is not present so that say, we cannot use with TTJ. We don't need
                // to fix now because once we figure out TTJ DP, I think this issue will be resolved.
                context.getPlanBuildContext().setOrderedGraph(null);
            }
        }
        // It's easier to just return a new Plan instead of modifying the existing plan
        Plan newPlan = createPhysicalPlanFromJoinOrdering(optimalJoinOrdering, context.getPlanBuildContext().getOrderedGraph());
        // Because multiple rules can be applied before this, we copy the existing plan planStatistics to the new plan so that we don't lose
        // the statistics coming from application of previous rules
        newPlan.setPlanStatistics(context.getPlanStatistics());
        ruleStatistics = builder.build();
        return newPlan;
    }

    public class DPTable
    {
        //TODO: we could optimize a bit here for the performance as we only need
        //the current level and the level below for the DP table
        Map<DPTableEntryId, DPTableEntry> dpTable = new Object2ObjectOpenHashMap<>();
        List<SchemaTableName> schemaTableNames;
        CatalogGroup catalogGroup;

        public DPTable(List<SchemaTableName> schemaTableNames, CatalogGroup catalogGroup)
        {
            this.schemaTableNames = schemaTableNames;
            this.catalogGroup = catalogGroup;
        }

        public DPTableEntry getDPEntry(DPTableEntryId id)
        {
            return dpTable.get(id);
        }

        @JsonIgnore
        public DPTableEntry getBestPlan()
        {
            CardEstContext context = CardEstContext.builder()
                    .setPlanBuildContext(ruleContext.getPlanBuildContext())
                    .build();
            return getBestPlanHelper(new HashSet<>(schemaTableNames), context);
        }

        private DPTableEntry getBestPlanHelper(Set<SchemaTableName> schemaTableNames,
                                               CardEstContext context)
        {
            if (Switches.DEBUG) {
                if (traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("schemaTableNames for getBestPlanHelper: " + schemaTableNames, traceDepth++));
                }
            }
            DPTableEntryId entryId = getDPTableEntryId(schemaTableNames);
            if (dpTable.containsKey(entryId)) {
                return dpTable.get(entryId);
            }
            if (schemaTableNames.size() == 1) {
                DPTableEntry dpTableEntry = DPTableEntry.builder(0,
                        new ArrayList<>(schemaTableNames),
                        costProvider.getSize(schemaTableNames, context)).build();
                dpTable.put(entryId, dpTableEntry);
                return dpTableEntry;
            }
            long optCost = Long.MAX_VALUE;
            List<List<SchemaTableName>> allCandidateSchemaTableNames = generateCandidateSchemaTableNames(new ArrayList<>(schemaTableNames));
            checkState(!allCandidateSchemaTableNames.isEmpty(),
                    "allCandidateSchemaTableNames from " + schemaTableNames + " is empty, which shouldn't be");
            List<DPTableEntry> tieBreakingEntries = new ArrayList<>();
            if (enableHJPhysicalHeuristics && allCandidateSchemaTableNames.get(0).size() == 1) {
                optCost = Long.MIN_VALUE;
            }
            for (List<SchemaTableName> candidateSchemaTableNames : allCandidateSchemaTableNames) {
                if (Switches.DEBUG) {
                    if (traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessageWithDepth("candidateSchemaTableNames: " + candidateSchemaTableNames, traceDepth++));
                    }
                }
                DPTableEntry entry = getBestPlanHelper(new HashSet<>(candidateSchemaTableNames), context);
                long entryOptCost;
                if (entry.planAssociatedWithOptCost.size() != 1) {
                    entryOptCost = entry.cardEstReturn.getSize() + entry.optCost;
                }
                else {
                    // Per the reference "not including the R_i's themselves"
                    entryOptCost = entry.optCost;
                }
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("entryOptCost: " + entryOptCost, traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("optCost: " + optCost, traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("enableHJPhysicalHeuristics: " + enableHJPhysicalHeuristics, traceDepth));
                }
                if (entryOptCost == optCost) {
                    tieBreakingEntries.add(entry);
                }
                if (enableHJPhysicalHeuristics && entry.planAssociatedWithOptCost.size() == 1) {
                    entryOptCost = entry.cardEstReturn.getSize() + entry.optCost;
                    if (entryOptCost > optCost) {
                        // We only apply enableHJPhysicalHeuristics for the row of DP that has two relations in each cell
                        tieBreakingEntries = new ArrayList<>();
                        tieBreakingEntries.add(entry);
                        optCost = entryOptCost;
                    }
                }
                else {
                    if (entryOptCost < optCost) {
                        tieBreakingEntries = new ArrayList<>();
                        tieBreakingEntries.add(entry);
                        optCost = entryOptCost;
                    }
                }
            }
            DPTableEntry bestEntry = tieBreak(tieBreakingEntries);
            DPTableEntry dpTableEntry = DPTableEntry.builder(optCost,
                    planAssociatedWithOptCost(bestEntry.planAssociatedWithOptCost, schemaTableNames),
                    costProvider.getSize(schemaTableNames, context)).build();
            checkState(dpTableEntry.planAssociatedWithOptCost != null,
                    entryId + " associated dpTableEntry has planAssociatedWithOptCost = null");
            dpTable.put(entryId, dpTableEntry);
            return dpTableEntry;
        }

        protected List<List<SchemaTableName>> generateCandidateSchemaTableNames(List<SchemaTableName> schemaTableNames)
        {
            if (Switches.DEBUG) {
                traceDepth++;
            }
            List<List<SchemaTableName>> allCandidateSchemaTableNames = new ArrayList<>();
            for (int leftOutIdx = 0; leftOutIdx < schemaTableNames.size(); ++leftOutIdx) {
                List<SchemaTableName> candidateSchemaTableNames = new ArrayList<>();
                HashMap<SchemaTableName, List<String>> schemaTableName2Attributes = new HashMap<>();
                for (int i = 0; i < schemaTableNames.size(); ++i) {
                    if (i != leftOutIdx) {
                        SchemaTableName schemaTableName = schemaTableNames.get(i);
                        candidateSchemaTableNames.add(schemaTableName);
                        schemaTableName2Attributes.put(schemaTableName, catalogGroup.getTableCatalog(schemaTableName).getAttributes());
                    }
                }
                if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                    traceLogger.debug(formatTraceMessageWithDepth("checking " + candidateSchemaTableNames +
                            " to see if it has a cross product free ordering", traceDepth));
                }
                if (checkIfCrossProductsFreeOrderingExists(candidateSchemaTableNames, schemaTableName2Attributes)) {
                    if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                        traceLogger.debug(formatTraceMessageWithDepth(candidateSchemaTableNames + " has cross product free ordering", traceDepth));
                    }
                    allCandidateSchemaTableNames.add(candidateSchemaTableNames);
                }
            }
            if (Switches.DEBUG) {
                traceDepth--;
            }
            return allCandidateSchemaTableNames;
        }

        protected List<SchemaTableName> planAssociatedWithOptCost(List<SchemaTableName> planAssociatedWithOptCostFromExistingEntry,
                                                                  Collection<SchemaTableName> schemaTableNames)
        {
            List<SchemaTableName> schemaTableNamesDifference = new ArrayList<>(CollectionUtils.disjunction(planAssociatedWithOptCostFromExistingEntry, schemaTableNames));
            checkState(schemaTableNamesDifference.size() == 1, "schemaTableNamesDifference doesn't contain only one element");
            List<SchemaTableName> returns = new ArrayList<>(planAssociatedWithOptCostFromExistingEntry);
            // We only generate left-deep plan. Thus, the order of returns gives the order of relations on the left-deep plan bottom-up.
            returns.add(schemaTableNamesDifference.get(0));
            return returns;
        }

        /**
         * Tie breaking for a list of cells have the same cost
         * <p>
         * For example, for {R,S} cell, since both {R} and {S} cell have the same cost (i.e., 0),
         * we need to have a way to break tie. Textbook breaks the tie by selecting the cell with smaller relation size.
         */
        private DPTableEntry tieBreak(List<DPTableEntry> dpTableEntries)
        {
            long bestSize = Long.MAX_VALUE;
            DPTableEntry bestTableEntry = null;
            for (DPTableEntry dpTableEntry : dpTableEntries) {
                if (dpTableEntry.cardEstReturn.getSize() < bestSize) {
                    bestSize = dpTableEntry.cardEstReturn.getSize();
                    bestTableEntry = dpTableEntry;
                }
            }
            return bestTableEntry;
        }

        @JsonValue
        @Override
        public String toString()
        {
            Map<DPTableEntryId, DPTableEntry> sortedMap = dpTable.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByKey())
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug("sortedMap in DP.toString(): " + sortedMap);
            }
            AsciiTable at = new AsciiTable();
            int currDPTableEntryIdNumSchemas = 0;
            int maxDPTableEntryIdNumSchemas = 0;
            List<List<String>> rows = new ArrayList<>();
            int maxLength = 0;
            List<Map.Entry<DPTableEntryId, DPTableEntry>> currEntries = new ArrayList<>();
            for (Map.Entry<DPTableEntryId, DPTableEntry> entry : sortedMap.entrySet()) {
                int schemaSize = entry.getKey().schemaTableNames.size();
                if (schemaSize != currDPTableEntryIdNumSchemas) {
                    if (schemaSize > maxDPTableEntryIdNumSchemas) {
                        maxDPTableEntryIdNumSchemas = schemaSize;
                    }
                    currDPTableEntryIdNumSchemas = schemaSize;
                    if (!currEntries.isEmpty()) {
                        List<String> row = currEntries
                                .stream()
                                .map(k -> constructDPCellInString(k.getKey(), k.getValue()))
                                .collect(Collectors.toList());
                        rows.add(row);
                        if (row.size() > maxLength) {
                            maxLength = row.size();
                        }
                        currEntries = new ArrayList<>();
                    }
                }
                currEntries.add(entry);
            }
            List<String> bottomRow = currEntries
                    .stream()
                    .map(k -> constructDPCellInString(k.getKey(), k.getValue()))
                    .collect(Collectors.toList());
            rows.add(bottomRow);
            if (bottomRow.size() > maxLength) {
                maxLength = bottomRow.size();
            }
            for (List<String> row : rows) {
                at.addRule();
                int currRowSize = row.size();
                if (currRowSize != maxLength) {
                    for (int i = 0; i < maxLength - currRowSize; i++) {
                        row.add("");
                    }
                }
                checkState(row.size() == maxLength, "row size: " + row.size() + "\n"
                        + "maxLength: " + maxLength);
                at.addRow(row);
            }
            at.addRule();
            at.setTextAlignment(TextAlignment.LEFT);
            return at.render(maxDPTableEntryIdNumSchemas * 100);
        }

        private String constructDPCellInString(DPTableEntryId dpTableEntryId, DPTableEntry dpTableEntry)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(dpTableEntryId.toString())
                    .append("<br>")
                    .append("---")
                    .append("<br>");
            stringBuilder.append("*size*: ")
                    .append(dpTableEntry.cardEstReturn.getSize())
                    .append("<br>");
            stringBuilder.append("*cost*: ")
                    .append(dpTableEntry.optCost)
                    .append("<br>");
            stringBuilder.append("*best plan*: ")
                    .append(dpTableEntry.planAssociatedWithOptCost)
                    .append("<br>");
            if (dpTableEntry.cardEstReturn.getJoinTree() != null) {
                stringBuilder.append("*join tree*: ")
                        .append("<br>")
                        .append(dpTableEntry.cardEstReturn.getJoinTree())
                        .append("<br>");
            }
            if (dpTableEntry.cardEstReturn.getCostReturn() != null) {
                JoinTreeCostReturn costReturn = dpTableEntry.cardEstReturn.getCostReturn();
                if (Switches.DEBUG && costReturn.getSqls() != null) {
                    stringBuilder.append("*SQLs for the join tree*: ")
                            .append("<br>")
                            .append(Joiner.on("<br>").join(costReturn.getSqls()))
                            .append("<br>");
                }
                stringBuilder.append("*CostModelNumberOfDanglingTuples*: ")
                        .append("<br>")
                        .append(costReturn.getCost() -
                                costReturn.getCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult() -
                                costReturn.getInnerRelationCleanStateSize())
                        .append("<br>");
                stringBuilder.append("*CostModelSizeOfIntermediateResultsThatArePartOfFinalJoinResult*: ")
                        .append("<br>")
                        .append(costReturn.getCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult())
                        .append("<br>");
                stringBuilder.append("*CostModelInnerRelationCleanStateSize*: ")
                        .append("<br>")
                        .append(costReturn.getInnerRelationCleanStateSize())
                        .append("<br>");
            }
            if (dpTableEntry.stats != null) {
                stringBuilder.append("*TTJIntermediateResultSize*: ")
                        .append("<br>")
                        .append(dpTableEntry.stats.ttjIntermediateResultSize)
                        .append("<br>");
                stringBuilder.append("*TTJNumberOfDanglingTuples*: ")
                        .append("<br>")
                        .append(dpTableEntry.stats.ttjNumberOfDanglingTuples)
                        .append("<br>");
                stringBuilder.append("*TTJSizeOfIntermediateResultsThatArePartOfFinalJoinResult*: ")
                        .append("<br>")
                        .append(dpTableEntry.stats.ttjSizeOfIntermediateResultsThatArePartOfFinalJoinResult)
                        .append("<br>");
                stringBuilder.append("*TTJInnerRelationCleanStateSize*: ")
                        .append("<br>")
                        .append(dpTableEntry.stats.ttjInnerRelationCleanStateSize)
                        .append("<br>");
            }
            return stringBuilder.toString();
        }
    }

    @Getter
    public static class DPTableEntry
    {
        // the optimal cost for computing this entry
        long optCost;
        // the plan associated with the optimal cost
        List<SchemaTableName> planAssociatedWithOptCost;
        CardEstReturn cardEstReturn;
        DPTableEntryStats stats;

        private DPTableEntry(Builder builder)
        {
            this.optCost = builder.optCost;
            this.planAssociatedWithOptCost = builder.planAssociatedWithOptCost;
            this.cardEstReturn = builder.cardEstReturn;
            this.stats = builder.stats;
        }

        public static Builder builder(long optCost, List<SchemaTableName> planAssociatedWithOptCost, CardEstReturn cardEstReturn)
        {
            return new Builder(optCost, planAssociatedWithOptCost, cardEstReturn);
        }

        @Override
        public String toString()
        {
            return "DPTableEntry{" +
                    "optCost=" + optCost +
                    ", planAssociatedWithOptCost=" + planAssociatedWithOptCost +
                    ", cardEstReturn=" + cardEstReturn +
                    ", stats=" + stats +
                    '}';
        }

        public static class Builder
        {
            long optCost;
            List<SchemaTableName> planAssociatedWithOptCost;
            CardEstReturn cardEstReturn;
            DPTableEntryStats stats;

            public Builder(long optCost, List<SchemaTableName> planAssociatedWithOptCost, CardEstReturn cardEstReturn)
            {
                this.optCost = optCost;
                this.planAssociatedWithOptCost = planAssociatedWithOptCost;
                this.cardEstReturn = cardEstReturn;
            }

            public Builder dpTableEntryStats(DPTableEntryStats dpTableEntryStats)
            {
                this.stats = dpTableEntryStats;
                return this;
            }

            public DPTableEntry build()
            {
                return new DPTableEntry(this);
            }
        }
    }

    public static class DPTableEntryStats
    {
        // The number of dangling tuples gathered by evaluating the plan associated with the entry using TTJ
        long ttjNumberOfDanglingTuples;
        // The total number of tuples produced by TTJ
        long ttjIntermediateResultSize;
        // The number of tuples produced during TTJ evaluation that are part of final join result
        long ttjSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        // The size of inner relations that are in clean state
        long ttjInnerRelationCleanStateSize;

        public static DPTableEntryStats of(long ttjNumberOfDanglingTuples,
                                           long ttjIntermediateResultSize,
                                           long ttjInnerRelationCleanStateSize)
        {
            DPTableEntryStats dpTableEntryStats = new DPTableEntryStats();
            dpTableEntryStats.ttjNumberOfDanglingTuples = ttjNumberOfDanglingTuples;
            dpTableEntryStats.ttjIntermediateResultSize = ttjIntermediateResultSize;
            dpTableEntryStats.ttjSizeOfIntermediateResultsThatArePartOfFinalJoinResult = ttjIntermediateResultSize - ttjNumberOfDanglingTuples;
            dpTableEntryStats.ttjInnerRelationCleanStateSize = ttjInnerRelationCleanStateSize;
            return dpTableEntryStats;
        }
    }

    public static class DPTableEntryId
            implements Comparable<DPTableEntryId>
    {
        // TODO: need a better way to implement it for the performance sake (probably want to have
        // schemaTableName has id as well when creating the logical plan (same purpose as nodeId for
        // multiwayJoinNode --> if multiwayJoinNode exists, use MultiwayJoinNode id as the id for schemaTableName
        // as well)
        Set<SchemaTableName> schemaTableNames;

        private DPTableEntryId(Set<SchemaTableName> schemaTableNames)
        {
            this.schemaTableNames = schemaTableNames;
        }

        public static DPTableEntryId getDPTableEntryId(Set<SchemaTableName> schemaTableNames)
        {
            return new DPTableEntryId(schemaTableNames);
        }

        @Override
        public int hashCode()
        {
            return schemaTableNames.hashCode();
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == this) {
                return true;
            }
            if (!(o instanceof DPTableEntryId)) {
                return false;
            }
            DPTableEntryId other = (DPTableEntryId) o;
            return this.schemaTableNames.equals(other.schemaTableNames);
        }

        @Override
        public int compareTo(DPTableEntryId o)
        {
            // sort in descending order
            return o.schemaTableNames.size() - schemaTableNames.size();
        }

        @Override
        public String toString()
        {
            return "{" + Joiner.on(",").join(schemaTableNames) + "}";
        }
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        // the rule works for HJ for now
        return node.isRoot() &&
                node.getNodeType() == OptType.join && // Root node of the given plan has to be a join
                (((JoinNode) node).getLeft().getNodeType() == OptType.join ||
                        (((JoinNode) node).getLeft().getNodeType() == OptType.table && ((JoinNode) node).getRight().getNodeType() == OptType.table)) && // The given plan is a left-deep plan
                !TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.isAssignableFrom(node.getOperator().getClass()); // enforce the rule cannot be applied to TTJ-family
    }

    @Override
    public RuleType getRuleType()
    {
        return RuleType.AS_A_WHOLE;
    }
}
