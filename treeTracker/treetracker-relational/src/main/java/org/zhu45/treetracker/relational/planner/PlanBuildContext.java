package org.zhu45.treetracker.relational.planner;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import lombok.Getter;
import lombok.Setter;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedJoinOperator;
import org.zhu45.treetracker.relational.operator.noGoodList.NoGoodList;
import org.zhu45.treetracker.relational.operator.noGoodList.PlainNoGoodList;
import org.zhu45.treetracker.relational.planner.catalog.CatalogGroup;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.Rule;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static com.google.inject.internal.util.Preconditions.checkArgument;
import static com.google.inject.internal.util.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;

/**
 * Information shared between logical plan and physical plan when build the plan. It also
 * includes information used only during the plan evaluation (i.e., by the operators).
 */
public class PlanBuildContext
{
    private PlanNodeIdAllocator idAllocator;
    @Getter
    private List<Rule> rules;
    @Getter
    private MultiwayJoinOrderedGraph orderedGraph;
    @Getter
    private List<MultiwayJoinNode> childrenOfRoot;
    @Getter
    private NoGoodList noGoodList;
    // The PlanNode that has an operator and the operator is the root of the operator tree.
    // Except Postgres plan, this is the root PlanNode of the plan. Under Postgres plan, this node
    // may not be the root of the plan because the root of the Postgres plan may not have a binding operator in our system, e.g., Aggregate.
    @Setter @Getter
    private PlanNode root;
    // The PlanNode that is the root of the loaded Postgres plan.
    @Setter @Getter
    private PlanNode postgresPlanRoot;
    @Getter
    @Setter
    private JdbcClient jdbcClient;
    @Getter
    private final Map<OptType, List<Class<? extends Operator>>> operatorMap;
    @Getter
    private CatalogGroup catalogGroup;
    @Getter
    @Setter
    private List<SchemaTableName> schemaTableNameList;
    @Getter
    private final SemiJoinOrdering semiJoinOrdering;
    @Setter
    @Getter
    private Class<? extends TupleBasedJoinOperator> semiJoinClazz;
    @Getter
    private Boolean disablePTOptimizationTrick;
    @Getter
    private Boolean enableJoinGraphHeuristicFromPT;
    @Getter
    private Boolean skipTopDownSemijoins;
    @Getter
    private PlanBuildOption planBuildOption;
    @Getter
    private String postgresPlan;
    @Getter // Only need to set when use Postgres plan because Postgres plan doesn't contain schema of the relation.
    private String schema;
    @Getter @Setter
    HashMap<SchemaTableName, HashSet<String>> schemaTableNameAttributes;

    private PlanBuildContext(Builder builder)
    {
        this.rules = builder.rules;
        this.orderedGraph = builder.orderedGraph;
        this.childrenOfRoot = builder.childrenOfRoot;
        this.idAllocator = builder.idAllocator;
        this.jdbcClient = builder.jdbcClient;
        this.noGoodList = requireNonNullElse(builder.noGoodList, PlainNoGoodList.create());
        this.operatorMap = builder.operatorMap;
        this.semiJoinOrdering = builder.semiJoinOrdering;
        this.semiJoinClazz = builder.semiJoinClazz;
        this.disablePTOptimizationTrick = builder.disablePTOptimizationTrick;
        this.enableJoinGraphHeuristicFromPT = builder.enableJoinGraphHeuristicFromPT;
        this.skipTopDownSemijoins = builder.skipTopDownSemijoins;
        this.planBuildOption = builder.planBuildOption;
        this.postgresPlan = builder.postgresPlan;
        this.schema = builder.schema;
        this.schemaTableNameList = builder.schemaTableNameList;
    }

    public PlanBuildContext(PlanBuildContext context, MultiwayJoinOrderedGraph orderedGraph)
    {
        this.rules = context.rules;
        setOrderedGraph(orderedGraph);
        this.root = context.root;
        this.postgresPlanRoot = context.postgresPlanRoot;
        this.catalogGroup = context.catalogGroup;
        this.idAllocator = context.idAllocator;
        this.jdbcClient = context.jdbcClient;
        this.noGoodList = requireNonNullElse(context.noGoodList, PlainNoGoodList.create());
        this.operatorMap = context.operatorMap;
        this.semiJoinOrdering = context.semiJoinOrdering;
        this.semiJoinClazz = context.semiJoinClazz;
        this.disablePTOptimizationTrick = context.disablePTOptimizationTrick;
        this.enableJoinGraphHeuristicFromPT = context.enableJoinGraphHeuristicFromPT;
        this.skipTopDownSemijoins = context.skipTopDownSemijoins;
        this.planBuildOption = context.planBuildOption;
        this.postgresPlan = context.postgresPlan;
        this.schema = context.schema;
        this.schemaTableNameList = context.schemaTableNameList;
        this.schemaTableNameAttributes = context.schemaTableNameAttributes;
    }

    public void setOrderedGraph(MultiwayJoinOrderedGraph orderedGraph)
    {
        this.orderedGraph = orderedGraph;
        MultiwayJoinNode root = orderedGraph.getRoot();
        childrenOfRoot = orderedGraph.getChildren().get(root);
        this.planBuildOption = PlanBuildOption.JOINTREE;
    }

    public PlanNodeIdAllocator getPlanNodeIdAllocator()
    {
        return idAllocator;
    }

    public void initializeCatalogGroup(List<SchemaTableName> schemaTableNameList)
    {
        requireNonNull(schemaTableNameList, "schemaTableNameList must not be null");
        catalogGroup = CatalogGroup.initializeCatalogGroup(schemaTableNameList, jdbcClient);
    }

    public Map<Integer, List<Integer>> getNodeId2FactTableJoinAttributeIdx(TableNode outerTable)
    {
        checkState(getCatalogGroup() != null);
        checkState(outerTable.getOperator() != null, outerTable + " should have a binding operator");
        Map<Integer, List<Integer>> nodeId2FactTableJoinAttributeIdx = new Int2ObjectOpenHashMap<>();
        List<MultiwayJoinNode> children = orderedGraph.getChildren().get(outerTable.getOperator().getMultiwayJoinNode());
        for (MultiwayJoinNode childNode : children) {
            int nodeId = childNode.getNodeId();
            List<Integer> factTableJoinAttributeIdx = populateFactTableJoinAttributeIdx(outerTable.getOperator().getMultiwayJoinNode(),
                    childNode);
            nodeId2FactTableJoinAttributeIdx.put(nodeId, factTableJoinAttributeIdx);
        }
        return nodeId2FactTableJoinAttributeIdx;
    }

    private List<Integer> populateFactTableJoinAttributeIdx(MultiwayJoinNode root, MultiwayJoinNode childNode)
    {
        // The index of attributes of the fact table that can join with the base relation connected with
        // the current join operator. For example, the fact table R(a,b) and the base relation S(a,c,b).
        // Then, factTableJoinAttributeIdx = [0,1] because attribute a of R appears at pos 0 and
        // attribute b of R appears at pos 1, which both attributes also appear in S as well.
        List<Integer> factTableJoinAttributeIdx = new ArrayList<>();
        List<String> factTableAttributes = getCatalogGroup().getTableCatalog(root.getSchemaTableName()).getAttributes();
        List<Type> factTableTypes = getCatalogGroup().getTableCatalog(root.getSchemaTableName()).getTypeList();
        List<String> baseRelationAttributes = getCatalogGroup().getTableCatalog(
                childNode.getSchemaTableName()).getAttributes();
        List<Type> baseRelationTypes = getCatalogGroup().getTableCatalog(childNode.getSchemaTableName()).getTypeList();

        for (int i = 0; i < factTableAttributes.size(); ++i) {
            for (int j = 0; j < baseRelationAttributes.size(); ++j) {
                if (factTableAttributes.get(i).equals(baseRelationAttributes.get(j)) && factTableTypes.get(i).equals(
                        baseRelationTypes.get(j))) {
                    factTableJoinAttributeIdx.add(i);
                }
            }
        }
        return factTableJoinAttributeIdx;
    }

    public void setPlanBuildOption(PlanBuildOption option)
    {
        this.planBuildOption = option;
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private PlanNodeIdAllocator idAllocator;
        private List<Rule> rules;
        private MultiwayJoinOrderedGraph orderedGraph;
        private JdbcClient jdbcClient;
        private NoGoodList noGoodList;
        private List<MultiwayJoinNode> childrenOfRoot;
        private Map<OptType, List<Class<? extends Operator>>> operatorMap;
        private SemiJoinOrdering semiJoinOrdering;
        private Class<? extends TupleBasedJoinOperator> semiJoinClazz;
        private boolean disablePTOptimizationTrick;
        private boolean enableJoinGraphHeuristicFromPT;
        private boolean skipTopDownSemijoins;
        private PlanBuildOption planBuildOption = PlanBuildOption.DEFAULT;
        private String postgresPlan;
        private String schema;
        private List<SchemaTableName> schemaTableNameList;

        public Builder()
        {
        }

        public Builder setOrderedGraph(MultiwayJoinOrderedGraph orderedGraph)
        {
            checkArgument(orderedGraph != null, "orderedGraph cannot be null");
            this.orderedGraph = orderedGraph;
            MultiwayJoinNode root = orderedGraph.getRoot();
            childrenOfRoot = orderedGraph.getChildren().get(root);
            this.planBuildOption = PlanBuildOption.JOINTREE;
            return this;
        }

        public Builder setPlanNodeIdAllocator(PlanNodeIdAllocator idAllocator)
        {
            this.idAllocator = idAllocator;
            return this;
        }

        public Builder setRules(List<Rule> rules)
        {
            this.rules = requireNonNull(rules, "rules is null");
            return this;
        }

        public Builder setJdbcClient(JdbcClient jdbcClient)
        {
            this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
            return this;
        }

        public Builder setNoGoodList(NoGoodList noGoodList)
        {
            this.noGoodList = noGoodList;
            return this;
        }

        public Builder setOperatorMap(Map<OptType, List<Class<? extends Operator>>> operatorMap)
        {
            this.operatorMap = operatorMap;
            return this;
        }

        public Builder setSemijoinOrdering(SemiJoinOrdering semiJoinOrdering)
        {
            this.semiJoinOrdering = semiJoinOrdering;
            return this;
        }

        public Builder setSemiJoinClazz(Class<? extends TupleBasedJoinOperator> semiJoinClazz)
        {
            this.semiJoinClazz = semiJoinClazz;
            return this;
        }

        public Builder disablePTOptimizationTrick(boolean disablePTOptimizationTrick)
        {
            this.disablePTOptimizationTrick = disablePTOptimizationTrick;
            return this;
        }

        public Builder enableJoinGraphHeuristicFromPT(boolean enableJoinGraphHeuristicFromPT)
        {
            this.enableJoinGraphHeuristicFromPT = enableJoinGraphHeuristicFromPT;
            return this;
        }

        public Builder skipTopDownSemijoins(boolean skipTopDownSemijoins)
        {
            this.skipTopDownSemijoins = skipTopDownSemijoins;
            return this;
        }

        public Builder planBuildOption(PlanBuildOption planBuildOption)
        {
            this.planBuildOption = planBuildOption;
            return this;
        }

        public Builder postgresPlan(String postgresPlan)
        {
            this.postgresPlan = postgresPlan;
            // To avoid large refactoring, we automatically set PlanBuildOption when serOrderedGraph(). However,
            // with introduced PlanBuildOption, we should explicitly set it regardless what other data we set, e.g.,
            // postgresPlan. To encourage this better usage, we don't automatically set PlanBuildOption here.
            return this;
        }

        public Builder schema(String schema)
        {
            this.schema = schema;
            return this;
        }

        public Builder schemaTableNameList(List<SchemaTableName> schemaTableNameList)
        {
            this.schemaTableNameList = schemaTableNameList;
            return this;
        }

        public PlanBuildContext build()
        {
            return new PlanBuildContext(this);
        }
    }

    /**
     * Controls how we want to build a plan
     */
    public enum PlanBuildOption
    {
        DEFAULT,       // build a left-deep plan
        JOINTREE,      // build a left-deep plan based on join tree
        POSTGRES       // build plan based on input Postgres plan
    }
}
