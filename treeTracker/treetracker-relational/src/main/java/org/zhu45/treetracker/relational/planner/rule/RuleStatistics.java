package org.zhu45.treetracker.relational.planner.rule;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.relational.planner.cost.CardEstProviderStatistics;

import java.util.HashMap;

/**
 * Any information obtained during applying rule. This information is to be consumed (merged) into PlanStatistics.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RuleStatistics
{
    private Class<? extends Rule> rule;
    @Getter
    // the join orderings explored (from FindTheBestJoinOrdering rule)
    private final HashMap<JoinOrdering, Float> searchedPlan;
    @Getter
    // the optimal join ordering
    private final JoinOrdering optimalJoinOrdering;
    @Getter
    // the join trees explored (from FindOptimalJoinTree)
    private final HashMap<MultiwayJoinOrderedGraph, Double> searchedJoinTrees;
    @Getter
    // the optimal join tree
    private final MultiwayJoinOrderedGraph optimalJoinTree;
    @Getter
    // measures the quality of the selected plan, i.e., cost, which can be determined by arbitrary method
    private final double cost;
    @Getter
    // rule name
    private final String ruleName;
    @JsonIgnore
    @Getter
    // DP table (from FindTheBestJoinOrderingWithDP)
    private final FindTheBestJoinOrderingWithDP.DPTable dpTable;
    @Getter
    private int dpTableNumCells;
    @Getter
    private final CardEstProviderStatistics cardEstProviderStatistics;
    @Getter
    private final SemiJoinOrdering semiJoinOrdering;
    @Getter
    private final SemiJoinOrdering topDownSemiJoinOrdering;

    private RuleStatistics(Builder builder)
    {
        this.rule = builder.rule;
        this.ruleName = builder.ruleName;
        this.searchedPlan = builder.searchedPlan;
        this.optimalJoinOrdering = builder.optimalJoinOrdering;
        this.searchedJoinTrees = builder.searchedJoinTrees;
        this.optimalJoinTree = builder.optimalJoinTree;
        this.cost = builder.cost;
        this.dpTable = builder.dpTable;
        if (this.dpTable != null) {
            this.dpTableNumCells = this.dpTable.dpTable.size();
        }
        this.cardEstProviderStatistics = builder.cardEstProviderStatistics;
        this.semiJoinOrdering = builder.semiJoinOrdering;
        this.topDownSemiJoinOrdering = builder.topDownSemiJoinOrdering;
    }

    public static Builder builder(Class<? extends Rule> rule)
    {
        return new Builder(rule);
    }

    public static class Builder
    {
        private Class<? extends Rule> rule;
        // the join orderings explored (from FindTheBestJoinOrdering rule)
        private HashMap<JoinOrdering, Float> searchedPlan;
        // the optimal join ordering
        private JoinOrdering optimalJoinOrdering;
        // the join trees explored (from FindOptimalJoinTree)
        private HashMap<MultiwayJoinOrderedGraph, Double> searchedJoinTrees;
        // the optimal join tree
        private MultiwayJoinOrderedGraph optimalJoinTree;
        // measures the quality of the selected plan, i.e., cost, which can be determined by arbitrary method
        private double cost;
        // rule name
        private final String ruleName;
        // DP table (from FindTheBestJoinOrderingWithDP)
        private FindTheBestJoinOrderingWithDP.DPTable dpTable;
        private CardEstProviderStatistics cardEstProviderStatistics;
        // bottom-up semijoin ordering
        private SemiJoinOrdering semiJoinOrdering;
        // top-down semijoin ordering
        private SemiJoinOrdering topDownSemiJoinOrdering;

        private Builder(Class<? extends Rule> rule)
        {
            this.rule = rule;
            this.ruleName = rule.getCanonicalName();
        }

        public Builder searchedPlan(HashMap<JoinOrdering, Float> searchedPlan)
        {
            this.searchedPlan = searchedPlan;
            return this;
        }

        public Builder searchedJoinTrees(HashMap<MultiwayJoinOrderedGraph, Double> searchedJoinTrees)
        {
            this.searchedJoinTrees = searchedJoinTrees;
            return this;
        }

        public Builder optimalJoinTree(MultiwayJoinOrderedGraph joinTree)
        {
            this.optimalJoinTree = joinTree;
            return this;
        }

        public Builder optimalJoinOrdering(JoinOrdering optimalJoinOrdering)
        {
            this.optimalJoinOrdering = optimalJoinOrdering;
            return this;
        }

        public Builder cost(double cost)
        {
            this.cost = cost;
            return this;
        }

        public Builder dpTable(FindTheBestJoinOrderingWithDP.DPTable dpTable)
        {
            this.dpTable = dpTable;
            return this;
        }

        public Builder cardEstProviderStatistics(CardEstProviderStatistics cardEstProviderStatistics)
        {
            this.cardEstProviderStatistics = cardEstProviderStatistics;
            return this;
        }

        public Builder semijoinOrdering(SemiJoinOrdering semiJoinOrdering)
        {
            this.semiJoinOrdering = semiJoinOrdering;
            return this;
        }

        public Builder topDownSemiJoinOrdering(SemiJoinOrdering semiJoinOrdering)
        {
            this.topDownSemiJoinOrdering = topDownSemiJoinOrdering;
            return this;
        }

        public RuleStatistics build()
        {
            return new RuleStatistics(this);
        }
    }
}
