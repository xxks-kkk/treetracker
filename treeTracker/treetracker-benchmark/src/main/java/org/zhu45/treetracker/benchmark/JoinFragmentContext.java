package org.zhu45.treetracker.benchmark;

import lombok.Getter;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TreeTrackerBFJoinOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerJoinOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerJoinV2Operator;
import org.zhu45.treetracker.relational.operator.TreeTrackerTableScanOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerTableScanV2Operator;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBaseTreeTrackerOneBetaTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedIntHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedSSBLIPHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedSSBLIPTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodList;
import org.zhu45.treetracker.relational.operator.noGoodList.PlainNoGoodList;
import org.zhu45.treetracker.relational.planner.rule.Rule;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

public class JoinFragmentContext
{
    private boolean isDevMode;
    private List<Rule> rules;
    private Map<OptType, List<Class<? extends Operator>>> operatorMap;
    private NoGoodList noGoodList;
    private JoinOperator algorithm;
    private String queryName;
    private boolean stopAfterFullReducer;
    private JdbcClient jdbcClient;
    @Getter
    private boolean disablePTOptimizationTrick;

    // Specific to the impact of backjumping experiments
    private long backjumpedRelationSize;
    private int numberOfBackJumpedRelations;

    public static Builder hashJoinContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedIntHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.HASH_JOIN);
    public static Builder lipContextBuilder = JoinFragmentContext.builder()
            // NOTE: since LIP is only on SSB, it's okay we use SSB-specific LIP implementation
            .setOperatorMap(createMap(Optional.of(TupleBasedSSBLIPTableScanOperator.class),
                    Optional.of(TupleBasedSSBLIPHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.LIP);
    public static Builder yannakakisContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.Yannakakis);
    public static Builder yannakakisBContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.YannakakisB);
    public static Builder yannakakisVContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.YannakakisVanilla);
    public static Builder yannakakis1PassContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedIntHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.Yannakakis1Pass);
    public static Builder pTOContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.PTO);
    public static Builder ttjHPContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                    Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP);
    public static Builder ttjHPNONGContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP_NO_NG);
    public static Builder ttjHPNoDPContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                    Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP_NO_DP);
    public static Builder ttjHPVanillaContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedHighPerfTreeTrackerOneBetaHashTableIntNoDPOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP_VANILLA);
    public static Builder ttjHPBFContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TreeTrackerBFJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP_BF);
    public static Builder ttjHPBGContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedHighPerfTableScanOperator.class),
                    Optional.of(TreeTrackerBFJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP_BG);
    public static Builder ttjContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBaseTreeTrackerOneBetaTableScanOperator.class),
                    Optional.of(TupleBaseTreeTrackerOneBetaHashTableOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJ);
    public static Builder ttjV2ContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TreeTrackerTableScanV2Operator.class),
                    Optional.of(TreeTrackerJoinV2Operator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJHP);
    public static Builder ttjV1ContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TreeTrackerTableScanOperator.class),
                    Optional.of(TreeTrackerJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.TTJV1);
    public static Builder semiJoinContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedLeftSemiHashJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.SemiJoin);
    public static Builder bloomSemiJoinContextBuilder = JoinFragmentContext.builder()
            .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                    Optional.of(TupleBasedLeftSemiBloomJoinOperator.class)))
            .setNoGoodList(PlainNoGoodList.create())
            .setAlgorithm(JoinOperator.BloomSemiJoin);

    private JoinFragmentContext(Builder builder)
    {
        this.jdbcClient = builder.jdbcClient;
        this.isDevMode = builder.isDevMode;
        this.rules = builder.rules;
        this.operatorMap = builder.operatorMap;
        this.noGoodList = requireNonNullElse(builder.noGoodList, PlainNoGoodList.create());
        this.numberOfBackJumpedRelations = builder.numberOfBackJumpedRelations;
        this.backjumpedRelationSize = builder.backjumpedRelationSize;
        this.algorithm = builder.algorithm;
        this.queryName = builder.queryName;
        this.disablePTOptimizationTrick = builder.disablePTOptimizationTrick;
    }

    public JdbcClient getJdbcClient()
    {
        return jdbcClient;
    }

    public boolean isDevMode()
    {
        return isDevMode;
    }

    public List<Rule> getRules()
    {
        return rules;
    }

    public Map<OptType, List<Class<? extends Operator>>> getOperatorMap()
    {
        return operatorMap;
    }

    public NoGoodList getNoGoodList()
    {
        return noGoodList;
    }

    public long getBackjumpedRelationSize()
    {
        return backjumpedRelationSize;
    }

    public int getNumberOfBackJumpedRelations()
    {
        return numberOfBackJumpedRelations;
    }

    public JoinOperator getAlgorithm()
    {
        return algorithm;
    }

    public String getQueryName()
    {
        return queryName;
    }

    public boolean getStopAfterFullReducer()
    {
        return stopAfterFullReducer;
    }

    public static JoinFragmentContext.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean isDevMode;
        private List<Rule> rules;
        private Map<OptType, List<Class<? extends Operator>>> operatorMap;
        private NoGoodList noGoodList;
        private long backjumpedRelationSize;
        private int numberOfBackJumpedRelations;
        private JoinOperator algorithm;
        private String queryName;
        private boolean stopAfterFullReducer;
        private JdbcClient jdbcClient;
        private boolean disablePTOptimizationTrick;

        public Builder()
        {
            rules = Collections.emptyList();
        }

        public Builder setJdbcClient(JdbcClient jdbcClient)
        {
            this.jdbcClient = jdbcClient;
            return this;
        }

        public Builder setIsDevMode(Boolean isDevMode)
        {
            this.isDevMode = requireNonNull(isDevMode, "isDevMode is null");
            return this;
        }

        public Builder setRules(List<Rule> rules)
        {
            this.rules = requireNonNull(rules, "rules is null");
            return this;
        }

        public Builder setOperatorMap(Map<OptType, List<Class<? extends Operator>>> operatorMap)
        {
            requireNonNull(operatorMap, "operatorMap is null");
            checkArgument(!operatorMap.isEmpty(), "operatorMap cannot be empty");
            this.operatorMap = operatorMap;
            return this;
        }

        public Builder setNoGoodList(NoGoodList noGoodList)
        {
            this.noGoodList = requireNonNull(noGoodList, "noGoodList is null");
            return this;
        }

        public Builder setBackJumpedRelationSize(long backjumpedRelationSize)
        {
            this.backjumpedRelationSize = backjumpedRelationSize;
            return this;
        }

        public Builder setNumberOfBackJumpedRelations(int numberOfBackJumpedRelations)
        {
            this.numberOfBackJumpedRelations = numberOfBackJumpedRelations;
            return this;
        }

        public Builder setAlgorithm(JoinOperator algorithm)
        {
            this.algorithm = algorithm;
            return this;
        }

        public Builder setQueryName(String queryName)
        {
            this.queryName = queryName;
            return this;
        }

        public Builder setStopAfterFullReducer(boolean stopAfterFullReducer)
        {
            this.stopAfterFullReducer = stopAfterFullReducer;
            return this;
        }

        public Builder disablePTOptimizationTrick(boolean disablePTOptimizationTrick)
        {
            this.disablePTOptimizationTrick = disablePTOptimizationTrick;
            return this;
        }

        public JoinFragmentContext build()
        {
            return new JoinFragmentContext(this);
        }
    }
}
