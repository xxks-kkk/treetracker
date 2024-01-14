package org.zhu45.treetracker.relational.planner.rule;

import com.fasterxml.jackson.annotation.JsonIgnore;
import de.renebergelt.test.Switches;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformationContext;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformationFactory;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.TTJAggregateStatisticsInformation;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.catalog.CatalogGroup;
import org.zhu45.treetracker.relational.planner.cost.CardEstContext;
import org.zhu45.treetracker.relational.planner.cost.CardEstProvider;
import org.zhu45.treetracker.relational.planner.cost.CardEstReturn;
import org.zhu45.treetracker.relational.planner.cost.CardEstType;
import org.zhu45.treetracker.relational.planner.cost.JoinTreeTrueCostProvider;
import org.zhu45.treetracker.relational.planner.cost.PostgresCardEstProvider;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph.getSchemaTableNameList;
import static org.zhu45.treetracker.common.Utils.formatTraceMessageWithDepth;
import static org.zhu45.treetracker.relational.execution.ExecutionBase.cleanUp;
import static org.zhu45.treetracker.relational.planner.Plan.getSchemaTableNames;
import static org.zhu45.treetracker.relational.planner.rule.FindTheBestJoinOrderingWithDP.DPTableEntryId.getDPTableEntryId;

/**
 * Play the same role as FindTheBestJoinOrderingWithDP but
 * works for TTJ family operators.
 * <p>
 * The implementation is based on Dan's idea: for a join of
 * R,S,T,U, for a cell {R,S,T}, we only consider all possible
 * join trees can be constructed from these three relations,
 * i.e., ignoring U. Then, we perform the costing of the join tree
 * without considering potential impact of U. We pick the lowest cost
 * join tree. Then, in cell {R,S,T,U}, we consider U.
 */
public class FindTheBestJoinOrderingJoinTreeWithDP
        extends FindTheBestJoinOrderingWithDP
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(FindTheBestJoinOrderingJoinTreeWithDP.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private boolean enableTTJExecutionForEachCell;
    private boolean enableHJPhysicalHeuristics;
    private RuleStatistics.Builder builder = RuleStatistics.builder(this.getClass());

    public FindTheBestJoinOrderingJoinTreeWithDP(CardEstProvider costProvider)
    {
        super();
        checkArgument(costProvider.getCardEstType() == CardEstType.TTJ);
        super.costProvider = costProvider;
    }

    public FindTheBestJoinOrderingJoinTreeWithDP(CardEstProvider costProvider, RuleConfiguration config)
    {
        super();
        checkArgument(costProvider.getCardEstType() == CardEstType.TTJ);
        super.costProvider = costProvider;
        updateConfig(config);
    }

    public void updateConfig(RuleConfiguration config)
    {
        this.enableTTJExecutionForEachCell = config.enableTTJExecutionForEachCell;
        this.enableHJPhysicalHeuristics = config.enableHJPhysicalHeuristics;
    }

    @Override
    public Plan applyToPhysicalPlan(PlanNode node, RuleContext context)
    {
        ruleContext = context;
        List<SchemaTableName> schemaTableNameList = getSchemaTableNames(node);
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.trace(formatTraceMessageWithDepth("schemaTableNameList for DPTable: " + schemaTableNameList, traceDepth));
        }
        DPTableForTTJ dpTable = new DPTableForTTJ(schemaTableNameList,
                context.getPlanBuildContext().getCatalogGroup());
        DPTableEntry tableEntry = dpTable.getBestPlan();
        JoinOrdering optimalJoinOrdering = new JoinOrdering(tableEntry.planAssociatedWithOptCost);
        builder = builder
                .cardEstProviderStatistics(costProvider.getStatistics())
                .cost(tableEntry.optCost)
                .dpTable(dpTable)
                .optimalJoinOrdering(optimalJoinOrdering)
                .semijoinOrdering(tableEntry.cardEstReturn.getCostReturn().getSemiJoinOrdering())
                .optimalJoinTree(tableEntry.cardEstReturn.getJoinTree());
        context.getPlanBuildContext().setOrderedGraph(tableEntry.cardEstReturn.getJoinTree());
        // It's easier to just return a new Plan instead of modifying the existing plan
        Plan newPlan = createPhysicalPlanFromJoinOrdering(optimalJoinOrdering, context.getPlanBuildContext().getOrderedGraph());
        // Because multiple rules can be applied before this, we copy the existing plan planStatistics to the new plan so that we don't lose
        // the statistics coming from application of previous rules
        newPlan.setPlanStatistics(context.getPlanStatistics());
        ruleStatistics = builder.build();
        return newPlan;
    }

    public class DPTableForTTJ
            extends DPTable
    {
        public DPTableForTTJ(List<SchemaTableName> schemaTableNames, CatalogGroup catalogGroup)
        {
            super(schemaTableNames, catalogGroup);
        }

        @JsonIgnore
        @Override
        public DPTableEntry getBestPlan()
        {
            CardEstContext context = CardEstContext.builder()
                    .setPlanBuildContext(ruleContext.getPlanBuildContext())
                    .build();
            return getBestPlanHelper(schemaTableNames, context);
        }

        private DPTableEntry getBestPlanHelper(List<SchemaTableName> schemaTableNames,
                                               CardEstContext context)
        {
            if (Switches.DEBUG) {
                traceDepth++;
                if (traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("schemaTableNames for getBestPlanHelper: " + schemaTableNames,
                            traceDepth));
                }
            }
            DPTableEntryId entryId = getDPTableEntryId(new HashSet<>(schemaTableNames));
            if (dpTable.containsKey(entryId)) {
                return dpTable.get(entryId);
            }
            if (schemaTableNames.size() == 1) {
                context.setTraceDepth(traceDepth);
                CardEstReturn cardEstReturn = costProvider.getSize(new HashSet<>(schemaTableNames), context);
                checkState(cardEstReturn.getJoinTree() != null);
                DPTableEntry dpTableEntry = DPTableEntry.builder(0,
                        cardEstReturn.getJoinOrdering().getSchemaTableNameList(),
                        cardEstReturn).build();
                dpTable.put(entryId, dpTableEntry);
                if (Switches.DEBUG) {
                    traceDepth--;
                }
                return dpTableEntry;
            }
            long optCost = Long.MAX_VALUE;
            List<List<SchemaTableName>> allCandidateSchemaTableNames = generateCandidateSchemaTableNames(new ArrayList<>(schemaTableNames));
            checkState(!allCandidateSchemaTableNames.isEmpty(),
                    "allCandidateSchemaTableNames from " + schemaTableNames + " is empty, which shouldn't be");
            List<DPTableEntry> tieBreakingEntries = new ArrayList<>();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("allCandidateSchemaTableNames: " + allCandidateSchemaTableNames, traceDepth));
            }
            if (enableHJPhysicalHeuristics && allCandidateSchemaTableNames.get(0).size() == 1) {
                optCost = Long.MIN_VALUE;
            }
            for (List<SchemaTableName> candidateSchemaTableNames : allCandidateSchemaTableNames) {
                if (Switches.DEBUG) {
                    if (traceLogger.isTraceEnabled()) {
                        traceLogger.trace(formatTraceMessageWithDepth("candidateSchemaTableNames: " + candidateSchemaTableNames, traceDepth));
                    }
                }
                DPTableEntry entry = getBestPlanHelper(candidateSchemaTableNames, context);
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("entry: " + entry, traceDepth));
                }
                // In this case, entry.cardEstReturn.getSize() is the cost of join tree, which is the summation of
                // dangling tuples produced before reaching clean state. Thus, we don't need to add entry.optCost (like
                // HJ DP) because that will lead to overcount. As a result, in DPTableEntry, "cost" represents the cost of
                // the join tree that the current cell is built from (i.e., "size" of some one-level-below-current cell)
                // and "size" represents the cost of the new join tree that the algorithm finds based on the previous join
                // tree, i.e., the one associated with this cell's "cost".
                long entryOptCost = entry.cardEstReturn.getSize();
                if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                    traceLogger.trace(formatTraceMessageWithDepth("entryOptCost: " + entryOptCost, traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("optCost: " + optCost, traceDepth));
                    traceLogger.trace(formatTraceMessageWithDepth("enableHJPhysicalHeuristics: " + enableHJPhysicalHeuristics, traceDepth));
                }
                if (entryOptCost == optCost) {
                    tieBreakingEntries.add(entry);
                }
                if (enableHJPhysicalHeuristics && entry.planAssociatedWithOptCost.size() == 1) {
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
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth(String.format("tieBreakingEntries (%s): %s", tieBreakingEntries.size(), tieBreakingEntries), traceDepth));
            }
            Pair<DPTableEntry, CardEstReturn> pair = tieBreak(tieBreakingEntries,
                    new HashSet<>(schemaTableNames), context);
            DPTableEntry.Builder dpTableEntryBuilder = DPTableEntry.builder(optCost,
                    planAssociatedWithOptCost(pair.getKey().planAssociatedWithOptCost, schemaTableNames),
                    pair.getValue());
            if (enableTTJExecutionForEachCell) {
                dpTableEntryBuilder.dpTableEntryStats(getDPTableEntryStatsFromTTJ(dpTableEntryBuilder.cardEstReturn.getJoinOrdering(),
                        dpTableEntryBuilder.cardEstReturn.getJoinTree(),
                        context.getPlanBuildContext()));
            }
            DPTableEntry dpTableEntry = dpTableEntryBuilder.build();
            checkState(dpTableEntry.planAssociatedWithOptCost != null,
                    entryId + " associated dpTableEntry has planAssociatedWithOptCost = null");
            checkState(dpTableEntry.planAssociatedWithOptCost.size() == schemaTableNames.size());
            checkState(getSchemaTableNameList(dpTableEntry.cardEstReturn.getJoinTree()).equals(dpTableEntry.planAssociatedWithOptCost),
                    "ordering from join tree:\n" + getSchemaTableNameList(dpTableEntry.cardEstReturn.getJoinTree()) + "\n"
                            + "entry ordering: " + dpTableEntry.planAssociatedWithOptCost);
            dpTable.put(entryId, dpTableEntry);
            if (Switches.DEBUG) {
                traceDepth--;
            }
            return dpTableEntry;
        }
    }

    /**
     * For {R,S} cell, since both {R} and {S} cell have the same cost (i.e., 0), we need to have a way to break tie.
     * Textbook breaks the tie by picking the cell with smaller relation size. But, unlike DP for HJ,
     * the join ordering has impact on the size. For example, if the size represents the size of dangling tuples
     * produced, then the size for {R} can be different. Currently, getSize() returns the size of R, which
     * assumes all tuples in R are dangling tuples (if return 0, then assumes non dangling tuples produced from
     * R), which can be inaccurate. Thus, we break the tie by picking the cell that gives the smaller size,
     * e.g., smaller number of dangling tuples produced, i.e., to pick {R} or {S}, we pick the one gives
     * the smaller size by getting estimate on R \join S, and S \join R, respectively.
     * <p>
     * Returns the selected DPTableEntry after tie breaking and CardEstReturn, which is the support evidence on
     * why the DPTableEntry returned get selected.
     */
    private Pair<DPTableEntry, CardEstReturn> tieBreak(List<DPTableEntry> dpTableEntries,
                                                       Set<SchemaTableName> schemaTableNames,
                                                       CardEstContext context)
    {
        checkArgument(!dpTableEntries.isEmpty(), "input dpTableEntries to tieBreak shouldn't be empty");
        if (Switches.DEBUG) {
            traceDepth++;
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("tieBreak called", traceDepth));
                traceLogger.trace(formatTraceMessageWithDepth("dpTableEntries: " + dpTableEntries, traceDepth));
                traceLogger.trace(formatTraceMessageWithDepth("schemaTableNames: " + schemaTableNames, traceDepth));
            }
        }
        CardEstReturn bestCardEstReturn = null;
        DPTableEntry bestEntry = null;
        long bestSize = Long.MAX_VALUE;
        List<Pair<DPTableEntry, CardEstReturn>> tieBreakingEntries = new ArrayList<>();
        for (DPTableEntry dpTableEntry : dpTableEntries) {
            context.setTraceDepth(traceDepth);
            context.setExistingJoinTree(dpTableEntry.cardEstReturn.getJoinTree());
            CardEstReturn cardEstReturn = costProvider.getSize(schemaTableNames, context);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("dpTableEntry: " + dpTableEntry, traceDepth));
                traceLogger.trace(formatTraceMessageWithDepth("cardEstReturn: " + cardEstReturn, traceDepth));
            }
            if (cardEstReturn.getSize() == bestSize) {
                tieBreakingEntries.add(Pair.of(dpTableEntry, cardEstReturn));
            }
            else if (cardEstReturn.getSize() < bestSize) {
                tieBreakingEntries = new ArrayList<>();
                tieBreakingEntries.add(Pair.of(dpTableEntry, cardEstReturn));
                bestCardEstReturn = cardEstReturn;
                bestSize = cardEstReturn.getSize();
                bestEntry = dpTableEntry;
            }
        }
        if (tieBreakingEntries.size() > 1) {
            Pair<DPTableEntry, CardEstReturn> bestPair = tieBreak2(tieBreakingEntries);
            bestEntry = bestPair.getLeft();
            bestCardEstReturn = bestPair.getRight();
        }
        if (Switches.DEBUG) {
            traceDepth--;
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("bestEntry: " + bestEntry, traceDepth));
                traceLogger.trace(formatTraceMessageWithDepth("bestCardEstReturn: " + bestCardEstReturn, traceDepth));
            }
        }
        return Pair.of(bestEntry, bestCardEstReturn);
    }

    /**
     * Break tie by selecting the cell with smaller size, which is on the join result size (e.g., for the cells from
     * the bottom row, the size would be relation size).
     */
    private Pair<DPTableEntry, CardEstReturn> tieBreak2(List<Pair<DPTableEntry, CardEstReturn>> dpTableEntries)
    {
        if (Switches.DEBUG) {
            traceDepth++;
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("tieBreak2 called", traceDepth));
                traceLogger.trace(formatTraceMessageWithDepth("dpTableEntries: " + dpTableEntries, traceDepth));
            }
        }
        PostgresCardEstProvider postgresCardEstProvider = new PostgresCardEstProvider(costProvider.isUseTrueCard());
        long bestSize = Long.MAX_VALUE;
        Pair<DPTableEntry, CardEstReturn> bestPair = null;
        for (Pair<DPTableEntry, CardEstReturn> pair : dpTableEntries) {
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("pair: " + pair, traceDepth));
            }
            DPTableEntry dpTableEntry = pair.getLeft();
            long joinSize = postgresCardEstProvider.getSize(new HashSet<>(dpTableEntry.planAssociatedWithOptCost), null).getSize();
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("joinSize: " + joinSize, traceDepth));
            }
            if (joinSize < bestSize) {
                bestSize = joinSize;
                bestPair = pair;
            }
        }
        if (Switches.DEBUG) {
            if (traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("bestPair: " + bestPair, traceDepth));
            }
            traceDepth--;
        }
        return bestPair;
    }

    private static DPTableEntryStats getDPTableEntryStatsFromTTJ(JoinOrdering joinOrdering,
                                                                 MultiwayJoinOrderedGraph joinTree,
                                                                 PlanBuildContext context)
    {
        checkState(Switches.STATS, "Switches.STATS has to be enabled");
        Plan plan = JoinTreeTrueCostProvider.createPhysicalPlanFromJoinOrdering(joinOrdering, joinTree, context);
        ExecutionNormal executionNormal = new ExecutionNormal(plan.getRoot());
        long resultSetSize = executionNormal.evalForBenchmarkWithResultSize();
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(plan.getRoot().getOperator())
                .setJoinOperator(JoinOperator.TTJHP)
                .setResultSetSize(resultSetSize)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        TTJAggregateStatisticsInformation aggregateStatisticsInformation = (TTJAggregateStatisticsInformation) factory.get();
        cleanUp(plan.getOperatorList());
        return DPTableEntryStats.of(aggregateStatisticsInformation.getNumberOfDanglingTuples(),
                aggregateStatisticsInformation.getTotalIntermediateResultsProducedWithoutNULL() +
                        aggregateStatisticsInformation.getResutSetSize(),
                aggregateStatisticsInformation.getInnerRelationSize() - aggregateStatisticsInformation.getTotalTuplesRemovedInnerRelations());
    }

    @Getter
    public static class RuleConfiguration
    {
        private final boolean enableTTJExecutionForEachCell;
        // The heuristics put the largest table as R_k (only impact the DP row of two relations in each cell)
        private final boolean enableHJPhysicalHeuristics;

        private RuleConfiguration(Builder builder)
        {
            this.enableTTJExecutionForEachCell = builder.enableTTJExecutionForEachCell;
            this.enableHJPhysicalHeuristics = builder.enableHJPhysicalHeuristics;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private boolean enableTTJExecutionForEachCell;
            private boolean enableHJPhysicalHeuristics;

            public Builder()
            {
            }

            public Builder enableTTJExecutionForEachCell(boolean enableTTJExecutionForEachCell)
            {
                this.enableTTJExecutionForEachCell = enableTTJExecutionForEachCell;
                return this;
            }

            public Builder enableHJPhysicalHeuristics(boolean enableHJPhysicalHeuristics)
            {
                this.enableHJPhysicalHeuristics = enableHJPhysicalHeuristics;
                return this;
            }

            public RuleConfiguration build()
            {
                return new RuleConfiguration(this);
            }
        }
    }

    @Override
    public boolean checkForRulePrecondition(PlanNode node)
    {
        return applicableTowardsTTJ(node) || applicableTowardsYannakakis(node);
    }

    private boolean applicableTowardsTTJ(PlanNode node)
    {
        return node.isRoot() &&
                node.getNodeType() == OptType.join && // Root node of the given plan has to be a join
                (((JoinNode) node).getLeft().getNodeType() == OptType.join ||
                        (((JoinNode) node).getLeft().getNodeType() == OptType.table && ((JoinNode) node).getRight().getNodeType() == OptType.table)) && // The given plan is a left-deep plan
                TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.isAssignableFrom(node.getOperator().getClass());
    }

    private boolean applicableTowardsYannakakis(PlanNode node)
    {
        return node.getNodeType() == OptType.fullReducer;
    }
}
