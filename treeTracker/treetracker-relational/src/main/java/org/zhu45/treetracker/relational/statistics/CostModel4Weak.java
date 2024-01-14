package org.zhu45.treetracker.relational.statistics;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.BaseNode;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.math.BigDecimal;
import java.util.HashMap;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Cost Model 4 weak assumption.
 */
public class CostModel4Weak
        implements CostModel
{
    CostModel4Statistics costModelStatistics;
    CostModelStatisticsData costModelStatisticsData;
    CostModelContext context;

    public CostModel4Weak(CostModelContext context)
    {
        this.context = context;
        costModelStatistics = new CostModel4Statistics(context);
        costModelStatisticsData = costModelStatistics.costModel4StatisticsData;
    }

    @Override
    public BigDecimal getCost(JoinOperator joinAlgorithm)
    {
        switch (joinAlgorithm) {
            case TTJ:
            case TTJHP:
                return getCostTTJ();
            case Yannakakis:
                return getCostYannakakis();
            default:
                throw new UnsupportedOperationException("No cost equation found for " + joinAlgorithm);
        }
    }

    private BigDecimal getCostYannakakis()
    {
        BigDecimal cost = BigDecimal.valueOf(0L);
        HashMap<MultiwayJoinNode, Integer> relationSizes = costModelStatisticsData.getRelationSizes();
        HashMap<MultiwayJoinNode, Long> fullyReducedRelationSizes = costModelStatisticsData.getFullyReducedRelationSizes();
        for (MultiwayJoinNode node : fullyReducedRelationSizes.keySet()) {
            if (node.getNodeType() == NodeType.Internal) {
                cost = cost.add(BigDecimal.valueOf(3 * fullyReducedRelationSizes.get(node)));
            }
            cost = cost.add(BigDecimal.valueOf(relationSizes.get(node)).add(BigDecimal.valueOf(2 * fullyReducedRelationSizes.get(node))));
        }
        for (MultiwayJoinNode node : fullyReducedRelationSizes.keySet()) {
            cost = cost.add(BigDecimal.valueOf(fullyReducedRelationSizes.get(node)));
        }
        cost = cost.add(BigDecimal.valueOf(context.getQuery().getNumRelations() - 1));
        checkArgument(cost.compareTo(BigDecimal.ZERO) > 0, "computed cost is smaller than 0");
        return cost;
    }

    private BigDecimal getCostTTJ()
    {
        BigDecimal cost = BigDecimal.valueOf(0);
        HashMap<MultiwayJoinNode, Integer> relationSizes = costModelStatisticsData.getRelationSizes();
        HashMap<MultiwayJoinNode, Float> semijoinSelectivities = costModelStatisticsData.getSemijoinSelectivities();
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Float>> antijoinSelectivities = costModelStatisticsData.getAntijoinSelectivities();
        HashMap<MultiwayJoinNode, Long> fullyReducedRelationSizes = costModelStatisticsData.getFullyReducedRelationSizes();
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr = costModelStatisticsData.getDSR();
        for (MultiwayJoinNode node : relationSizes.keySet()) {
            switch (node.getNodeType()) {
                case Internal:
                    cost = cost.add(BigDecimal.valueOf(relationSizes.get(node)));
                    for (BaseNode childBaseNode : context.getOrderedGraph().getChildren().get(node)) {
                        cost = cost.add(BigDecimal.valueOf(relationSizes.get(node) * semijoinSelectivities.get(node) * antijoinSelectivities.get(node).get(childBaseNode) * dsr.get(node).get(childBaseNode)));
                    }
                    break;
                case Root:
                    int dmax = 0;
                    for (BaseNode childBaseNode : context.getOrderedGraph().getChildren().get(node)) {
                        dmax = Math.max(dmax, dsr.get(node).get(childBaseNode));
                    }
                    cost = cost.add(BigDecimal.valueOf((relationSizes.get(node) - fullyReducedRelationSizes.get(node)) * dmax));
                    break;
                default:
                    cost = cost.add(BigDecimal.valueOf(relationSizes.get(node)));
            }
        }
        cost = cost.add(BigDecimal.valueOf(context.getQuery().getNumRelations() - 1));
        checkArgument(cost.compareTo(BigDecimal.ZERO) > 0, "computed cost is smaller than 0");
        return cost;
    }
}
