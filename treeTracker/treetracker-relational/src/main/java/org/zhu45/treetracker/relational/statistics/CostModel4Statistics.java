package org.zhu45.treetracker.relational.statistics;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.FullReducerStatisticsInformation;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.zhu45.treetracker.relational.operator.TTJAggregateStatisticsInformation.generateDSR;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.constructOrderedGraph;

public class CostModel4Statistics
        implements CostModelStatistics
{
    CostModelContext context;
    CostModel4StatisticsData costModel4StatisticsData;
    CostModelDataRepository costModelDataRepository = new CostModelDataRepository();

    public CostModel4Statistics(CostModelContext context)
    {
        this.context = context;
        costModel4StatisticsData = new CostModel4StatisticsData(context);
        costModelDataRepository.save(context.getQuery().getQueryName(), costModel4StatisticsData);
    }

    @Override
    public CostModelStatisticsData getCostModelStatisticsData()
    {
        return costModel4StatisticsData;
    }

    private static class CostModel4StatisticsData
            implements CostModelStatisticsData
    {
        HashMap<MultiwayJoinNode, Integer> relationSizes;
        // semijoin selectivity of R w.r.t its parent, i.e., \rho_R^{R^p}.
        // Only internal nodes of the join tree present in the map.
        HashMap<MultiwayJoinNode, Float> semijoinSelectivities;
        // antijoin selectivity of R w.r.t its children, i.e., \gamma_R^S.
        // Only internal nodes of the join tree present in the map.
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Float>> antijoinSelectivities;
        // fully reduced relation size
        HashMap<MultiwayJoinNode, Long> fullyReducedRelationSizes;
        // d_R^S
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr;
        CostModelContext context;

        @JsonCreator
        public CostModel4StatisticsData(
                @JsonProperty("relationSizes") HashMap<MultiwayJoinNode, Integer> relationSizes,
                @JsonProperty("semijoinSelectivities") HashMap<MultiwayJoinNode, Float> semijoinSelectivities,
                @JsonProperty("antijoinSelectivities") HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Float>> antijoinSelectivities,
                @JsonProperty("fullyReducedRelationSizes") HashMap<MultiwayJoinNode, Long> fullyReducedRelationSizes,
                @JsonProperty("dsr") HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr)
        {
            this.relationSizes = relationSizes;
            this.semijoinSelectivities = semijoinSelectivities;
            this.antijoinSelectivities = antijoinSelectivities;
            this.fullyReducedRelationSizes = fullyReducedRelationSizes;
            this.dsr = dsr;
        }

        public CostModel4StatisticsData(CostModelContext context)
        {
            this.context = context;
            getRelationSizes();
            getSemijoinSelectivities();
            getAntijoinSelectivities();
            getFullyReducedRelationSizes();
            getDSR();
        }

        @Override
        public HashMap<MultiwayJoinNode, Integer> getRelationSizes()
        {
            if (relationSizes == null) {
                relationSizes = new HashMap<>();
                JdbcClient jdbcClient = context.getJdbcClient();
                try (Connection connection = jdbcClient.getConnection()) {
                    for (MultiwayJoinNode node : context.getMultiwayJoinNodesInPlan()) {
                        SchemaTableName schemaTableName = node.getSchemaTableName();
                        JdbcTableHandle jdbcTableHandle = jdbcClient.getTableHandle(schemaTableName);
                        relationSizes.put(node, jdbcClient.getTableSize(connection, jdbcTableHandle));
                    }
                }
                catch (SQLException e) {
                    throw new TreeTrackerException(JDBC_ERROR, e);
                }
            }
            return relationSizes;
        }

        @Override
        public HashMap<MultiwayJoinNode, Float> getSemijoinSelectivities()
        {
            if (semijoinSelectivities == null) {
                semijoinSelectivities = new HashMap<>();
                for (MultiwayJoinNode node : context.getMultiwayJoinNodesInPlan()) {
                    if (node.getNodeType() == NodeType.Internal) {
                        semijoinSelectivities.put(node, getSemijoinSelectivitiesForInternalNodes(node));
                    }
                }
            }
            return semijoinSelectivities;
        }

        @Override
        public HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Float>> getAntijoinSelectivities()
        {
            if (antijoinSelectivities == null) {
                antijoinSelectivities = new HashMap<>();
                for (MultiwayJoinNode node : context.getMultiwayJoinNodesInPlan()) {
                    if (node.getNodeType() == NodeType.Internal) {
                        antijoinSelectivities.put(node, getAntijoinSelectivitiesForInternalNodes(node));
                    }
                }
            }
            return antijoinSelectivities;
        }

        @Override
        public HashMap<MultiwayJoinNode, Long> getFullyReducedRelationSizes()
        {
            if (fullyReducedRelationSizes == null) {
                fullyReducedRelationSizes = new HashMap<>();
                context.getQuery().eval();
                Operator rootOperator = context.getQuery().getRootOperator();
                FullReducerStatisticsInformation statisticsInformation = ((FullReducerStatisticsInformation) rootOperator.getStatisticsInformation());
                Map<String, Long> numberOfTuplesRemovedByFullReducer = statisticsInformation.getNumberOfTuplesRemovedByFullReducer();
                getRelationSizes();
                for (MultiwayJoinNode node : relationSizes.keySet()) {
                    fullyReducedRelationSizes.put(node, relationSizes.get(node) - numberOfTuplesRemovedByFullReducer.get(node.getNodeName()));
                }
            }
            return fullyReducedRelationSizes;
        }

        @Override
        public HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> getDSR()
        {
            if (dsr == null) {
                dsr = generateDSR(context.getOrderedGraph());
            }
            return dsr;
        }

        private float getSemijoinSelectivitiesForInternalNodes(MultiwayJoinNode node)
        {
            MultiwayJoinNode parentNode = context.getOrderedGraph().getParent().get(node).get(0);
            Plan plan = createPlanFromMultiwayJoinNodes(node, parentNode);
            ExecutionNormal executionNormal = new ExecutionNormal(plan.getRoot());
            long semijoinResultSize = executionNormal.evalForBenchmarkWithResultSize();
            getRelationSizes();
            int nodeRelationSize = relationSizes.get(node);
            return (float) semijoinResultSize / nodeRelationSize;
        }

        private HashMap<MultiwayJoinNode, Float> getAntijoinSelectivitiesForInternalNodes(MultiwayJoinNode node)
        {
            HashMap<MultiwayJoinNode, Float> antijoinSelectivities = new HashMap<>();
            List<MultiwayJoinNode> childNodes = context.getOrderedGraph()
                    .getChildren().get(node)
                    .stream()
                    .map(childNode -> (MultiwayJoinNode) childNode)
                    .collect(Collectors.toList());
            for (MultiwayJoinNode childNode : childNodes) {
                Plan plan = createPlanFromMultiwayJoinNodes(node, childNode);
                ExecutionNormal executionNormal = new ExecutionNormal(plan.getRoot());
                long semijoinResultSize = executionNormal.evalForBenchmarkWithResultSize();
                getRelationSizes();
                int nodeRelationSize = relationSizes.get(node);
                float semijoinSelectivity = (float) semijoinResultSize / nodeRelationSize;
                antijoinSelectivities.put(childNode, 1 - semijoinSelectivity);
            }
            return antijoinSelectivities;
        }

        private Plan createPlanFromMultiwayJoinNodes(MultiwayJoinNode parent, MultiwayJoinNode child)
        {
            MultiwayJoinOrderedGraph orderedGraph = constructOrderedGraph(parent, List.of(child));
            PlanBuildContext planBuildContext = builder()
                    .setOrderedGraph(orderedGraph)
                    .setRules(Collections.emptyList())
                    .setPlanNodeIdAllocator(context.getPlanNodeIdAllocator())
                    .setJdbcClient(context.getJdbcClient())
                    .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class), Optional.of(TupleBasedLeftSemiHashJoinOperator.class)))
                    .build();
            PlanBuilder planBuilder = new PlanBuilder(planBuildContext);
            Plan logicalPlan = planBuilder.build();
            RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(planBuildContext);
            Plan physicalPlan = physicalPlanBuilder.build(logicalPlan.getRoot());
            // TODO: need to clean up the resource
            return physicalPlan;
        }

        @Override
        @JsonIgnore
        public String getId()
        {
            return this.getClass().getSimpleName();
        }

        @Override
        public void setContext(CostModelContext context)
        {
            this.context = context;
        }
    }
}
