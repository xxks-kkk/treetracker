package org.zhu45.treetracker.relational.planner.cost;

import de.renebergelt.test.Switches;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.planner.JoinTreeGenerator;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode.getTableNode;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph.getSchemaTableNameList;
import static org.zhu45.treetracker.common.Utils.formatTraceMessageWithDepth;

/**
 * Provide card. estimate for TTJ
 */
public class TTJCardEstProvider
        implements CardEstProvider
{
    private static final Logger traceLogger;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(TTJCardEstProvider.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    JoinTreeCostProvider joinTreeCostProvider;

    public TTJCardEstProvider(JoinTreeCostProvider joinTreeCostProvider)
    {
        this.joinTreeCostProvider = joinTreeCostProvider;
    }

    @Override
    public CardEstReturn getSize(Set<SchemaTableName> schemaTableNames, CardEstContext context)
    {
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessageWithDepth("schemaTableNames: " + schemaTableNames
                    + " join tree: " + context.getExistingJoinTree(), context.getTraceDepth() + 1));
        }
        JoinTreeGenerator joinTreeGenerator = new JoinTreeGenerator(context.getPlanBuildContext());
        if (schemaTableNames.size() == 1) {
            // for single relation, return relation size because the size (e.g., size of dangling tuples)
            // of a single relation doesn't impact the cost in DP
            SchemaTableName schemaTableName = new ArrayList<>(schemaTableNames).get(0);
            long tableSize = context.getPlanBuildContext()
                    .getCatalogGroup()
                    .getTableCatalog(schemaTableName)
                    .getSize();
            MultiwayJoinOrderedGraph bestJoinTree = joinTreeGenerator.createSingleNodeJoinTree(schemaTableName);
            if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
                traceLogger.trace(formatTraceMessageWithDepth("best join Tree: " + bestJoinTree, context.getTraceDepth() + 1));
            }
            return CardEstReturn.builder(tableSize)
                    .setJoinTree(bestJoinTree)
                    .setJoinOrdering(new JoinOrdering(new ArrayList<>(schemaTableNames)))
                    .build();
        }

        // Core logic:
        // given existing join tree R-S and existing join ordering R,S, schemaTableNames {R,S,T},
        // we generate all possible join trees by attaching T to all possible places. Then, pick
        // the tree with the lowest cost and return.
        MultiwayJoinOrderedGraph existingJoinTree = new MultiwayJoinOrderedGraph(requireNonNull(context.getExistingJoinTree()));
        List<SchemaTableName> existingJoinOrdering = getSchemaTableNameList(existingJoinTree);

        List<SchemaTableName> schemaTableNamesDifference = new ArrayList<>(
                CollectionUtils.disjunction(existingJoinOrdering, schemaTableNames));
        checkState(schemaTableNamesDifference.size() == 1,
                "schemaTableNamesDifference: " + schemaTableNamesDifference + "\n"
                        + "existingJoinOrdering: " + getSchemaTableNameList(existingJoinTree) + "\n"
                        + "schemaTableNames: " + schemaTableNames);

        List<SchemaTableName> newSchemaTableNames = new ArrayList<>(existingJoinOrdering);
        newSchemaTableNames.add(schemaTableNamesDifference.get(0));
        JoinOrdering newJoinOrdering = new JoinOrdering(newSchemaTableNames);

        MultiwayJoinNode nodeToAttach = getTableNode(schemaTableNamesDifference.get(0),
                context.getPlanBuildContext().getJdbcClient());

        List<MultiwayJoinOrderedGraph> candidateJoinTrees = joinTreeGenerator
                .createAllPossibleJoinTreesThatAttachGivenNodeToGivenTree(existingJoinTree, nodeToAttach);

        MultiwayJoinOrderedGraph bestJoinTree = null;
        long bestSize = Long.MAX_VALUE;
        JoinTreeCostReturn bestCostReturn = null;
        for (MultiwayJoinOrderedGraph candidateJoinTree : candidateJoinTrees) {
            checkState(getSchemaTableNameList(candidateJoinTree)
                    .equals(newJoinOrdering.getSchemaTableNameList()));
            JoinTreeCostReturn costReturn = joinTreeCostProvider.getCost(newJoinOrdering,
                    candidateJoinTree, context.getPlanBuildContext());
            long joinTreeCost = (long) costReturn.getCost();
            if (joinTreeCost < bestSize) {
                bestSize = joinTreeCost;
                bestJoinTree = candidateJoinTree;
                bestCostReturn = costReturn;
            }
        }
        if (Switches.DEBUG && traceLogger.isTraceEnabled()) {
            traceLogger.trace(formatTraceMessageWithDepth("bestJoinTree: " + bestJoinTree, context.getTraceDepth() + 1));
        }
        // Note, if to implement DP in my idea, we need to return a priority queue by best join tree cost
        // due to corner case
        return CardEstReturn.builder(bestSize)
                .setJoinTree(bestJoinTree)
                .setJoinOrdering(newJoinOrdering)
                .setCostReturn(bestCostReturn)
                .build();
    }

    @Override
    public CardEstType getCardEstType()
    {
        return CardEstType.TTJ;
    }

    @Override
    public boolean isUseTrueCard()
    {
        return joinTreeCostProvider.isUseTrueCard();
    }

    @Override
    public CardEstProviderStatistics getStatistics()
    {
        return CardEstProviderStatistics.builder(this.getClass())
                .joinTreeCostStatistics(joinTreeCostProvider.getStatistics())
                .build();
    }
}
