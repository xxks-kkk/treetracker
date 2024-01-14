package org.zhu45.treetracker.relational.planner;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
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
import org.zhu45.treetracker.relational.planner.rule.Rule;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.util.ArrayList;
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
    private List<Rule> rules;
    private MultiwayJoinOrderedGraph orderedGraph;
    private List<MultiwayJoinNode> childrenOfRoot;
    private NoGoodList noGoodList;
    private PlanNode root;
    private Operator leftMostPlanNodeOperator;
    private JdbcClient jdbcClient;
    private final Map<OptType, List<Class<? extends Operator>>> operatorMap;
    private CatalogGroup catalogGroup;
    private List<SchemaTableName> schemaTableNameList;
    private Map<Integer, List<Integer>> nodeId2FactTableJoinAttributeIdx;
    private final SemiJoinOrdering semiJoinOrdering;
    private Class<? extends TupleBasedJoinOperator> semiJoinClazz;
    private Boolean disablePTOptimizationTrick;
    private Boolean enableJoinGraphHeuristicFromPT;

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
    }

    public void setRoot(PlanNode root)
    {
        this.root = root;
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public void setJdbcClient(JdbcClient jdbcClient)
    {
        this.jdbcClient = jdbcClient;
    }

    public JdbcClient getJdbcClient()
    {
        return this.jdbcClient;
    }

    public void setLeftMostPlanNodeOperator()
    {
        requireNonNull(root.getOperator(), "There is no operator in root. Maybe passed in logical plan instead of physical plan?");

        PlanNode current = root;
        while (!current.getSources().isEmpty()) {
            current = current.getSources().get(0);
        }
        leftMostPlanNodeOperator = current.getOperator();
        leftMostPlanNodeOperator.setLeftMostOperatorInPlan();
        checkArgument(leftMostPlanNodeOperator.getOperatorType() == OptType.table, "left most operator should be a table scan");
    }

    public Operator getLeftMostPlanNodeOperator()
    {
        return leftMostPlanNodeOperator;
    }

    public MultiwayJoinOrderedGraph getOrderedGraph()
    {
        return orderedGraph;
    }

    public void setOrderedGraph(MultiwayJoinOrderedGraph orderedGraph)
    {
        this.orderedGraph = orderedGraph;
    }

    public List<MultiwayJoinNode> getChildrenOfRoot()
    {
        return childrenOfRoot;
    }

    public NoGoodList getNoGoodList()
    {
        return noGoodList;
    }

    public List<Rule> getRules()
    {
        return rules;
    }

    public PlanNodeIdAllocator getPlanNodeIdAllocator()
    {
        return idAllocator;
    }

    public Map<OptType, List<Class<? extends Operator>>> getOperatorMap()
    {
        return operatorMap;
    }

    public void initializeCatalogGroup(List<SchemaTableName> schemaTableNameList)
    {
        catalogGroup = CatalogGroup.initializeCatalogGroup(schemaTableNameList, jdbcClient);
    }

    public CatalogGroup getCatalogGroup()
    {
        return catalogGroup;
    }

    public void setSchemaTableNameList(List<SchemaTableName> schemaTableNameList)
    {
        this.schemaTableNameList = schemaTableNameList;
    }

    public List<SchemaTableName> getSchemaTableNameList()
    {
        return schemaTableNameList;
    }

    public void setNodeId2FactTableJoinAttributeIdx()
    {
        nodeId2FactTableJoinAttributeIdx = new Int2ObjectOpenHashMap<>();
        checkState(orderedGraph != null);
        checkState(childrenOfRoot != null);
        checkState(getCatalogGroup() != null);
        for (MultiwayJoinNode childNode : childrenOfRoot) {
            int nodeId = childNode.getNodeId();
            List<Integer> factTableJoinAttributeIdx = populateFactTableJoinAttributeIdx(orderedGraph.getRoot(), childNode);
            nodeId2FactTableJoinAttributeIdx.put(nodeId, factTableJoinAttributeIdx);
        }
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
        List<String> baseRelationAttributes = getCatalogGroup().getTableCatalog(childNode.getSchemaTableName()).getAttributes();
        List<Type> baseRelationTypes = getCatalogGroup().getTableCatalog(childNode.getSchemaTableName()).getTypeList();

        for (int i = 0; i < factTableAttributes.size(); ++i) {
            for (int j = 0; j < baseRelationAttributes.size(); ++j) {
                if (factTableAttributes.get(i).equals(baseRelationAttributes.get(j)) &&
                        factTableTypes.get(i).equals(baseRelationTypes.get(j))) {
                    factTableJoinAttributeIdx.add(i);
                }
            }
        }
        return factTableJoinAttributeIdx;
    }

    public Map<Integer, List<Integer>> getNodeId2FactTableJoinAttributeIdx()
    {
        return nodeId2FactTableJoinAttributeIdx;
    }

    public SemiJoinOrdering getSemiJoinOrdering()
    {
        return semiJoinOrdering;
    }

    public Class<? extends TupleBasedJoinOperator> getSemiJoinClazz()
    {
        return semiJoinClazz;
    }

    public void setSemiJoinClazz(Class<? extends TupleBasedJoinOperator> semiJoinClazz)
    {
        this.semiJoinClazz = semiJoinClazz;
    }

    public Boolean getDisablePTOptimizationTrick()
    {
        return disablePTOptimizationTrick;
    }

    public Boolean getEnableJoinGraphHeuristicFromPT()
    {
        return enableJoinGraphHeuristicFromPT;
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

        public Builder()
        {
        }

        public Builder setOrderedGraph(MultiwayJoinOrderedGraph orderedGraph)
        {
            checkArgument(orderedGraph != null, "orderedGraph cannot be null");
            this.orderedGraph = orderedGraph;
            MultiwayJoinNode root = orderedGraph.getRoot();
            childrenOfRoot = orderedGraph.getChildren().get(root);
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

        public PlanBuildContext build()
        {
            return new PlanBuildContext(this);
        }
    }
}
