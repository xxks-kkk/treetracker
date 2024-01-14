package org.zhu45.treetracker.benchmark.statsgen;

import com.google.common.base.Joiner;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.PostgresNaturalJoinExecutor;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.tpch.TPCHQueriesAutoGen;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcColumnHandle;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.testing.Database;
import org.zhu45.treetracker.jdbc.testing.TestingDatabase;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftAntiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.PlanBuilder;
import org.zhu45.treetracker.relational.planner.PlanNode;
import org.zhu45.treetracker.relational.planner.PlanNodeIdAllocator;
import org.zhu45.treetracker.relational.planner.PlanVisitor;
import org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder;
import org.zhu45.treetracker.relational.planner.plan.FullReducerNode;
import org.zhu45.treetracker.relational.planner.plan.JoinNode;
import org.zhu45.treetracker.relational.planner.plan.TableNode;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.naturalJoinJdbcClientSupplier;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.PlanBuildContext.builder;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;
import static org.zhu45.treetracker.relational.planner.rule.AttachFullReducer.constructOrderedGraph;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

/**
 * We want to collect SemiJoinModRatio for the given input join tree
 */
public class CollectSemiJoinModRatio
{
    MultiwayJoinOrderedGraph joinTree;
    JoinOrdering joinOrdering;
    Map<SchemaTableName, Double> ratio;
    PlanNodeIdAllocator planNodeIdAllocator = new PlanNodeIdAllocator();
    JdbcClient jdbcClient = postgresJdbcClientSupplier.get();
    HashMap<MultiwayJoinNode, Float> semiJoinModRatioResult;
    Database database;

    public CollectSemiJoinModRatio(MultiwayJoinOrderedGraph joinTree,
                                   JoinOrdering joinOrdering)
    {
        this.joinTree = joinTree;
        this.joinOrdering = joinOrdering;
        ratio = new HashMap<>();
        this.database = new CSJMDB(naturalJoinJdbcClientSupplier.get());
    }

    public HashMap<MultiwayJoinNode, Float> getSemiJoinModRatioResult()
    {
        semiJoinModRatioResult = new HashMap<>();
        for (MultiwayJoinNode node : joinTree.getChildren().keySet()) {
            // collect for internal non-root node only
            if (node.getNodeType() == NodeType.Internal) {
                semiJoinModRatioResult.put(node, run(node));
            }
        }
        return semiJoinModRatioResult;
    }

    /***
     * Consider the following join tree:
     * R
     * |S
     * |T
     * ||U
     * ||G
     * The target node is T. Then, we want to compute
     * \semijoinmodratio of T =
     * (1) |T \leftsemijoin (R \join S) \leftsemijoin (U \antijoin \widetilde(U))| / |T| +
     * (2) |T \leftsemijoin (R \join S) \leftsemijoin \widetilde(U) \leftsemijoin (G \antijoin \widetilde(G))| / |T|
     */
    public float run(MultiwayJoinNode targetNode)
    {
        Plan plan = createPhysicalPlanFromJoinOrdering(this.joinOrdering, this.joinTree,
                TupleBasedTableScanOperator.class, TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class);
        float targetRelationSize = (float) plan.getRoot().getOperator().getPlanBuildContext().getCatalogGroup().getTableCatalog(targetNode.getSchemaTableName()).getSize();
        Map<SchemaTableName, SchemaTableName> cleanStateRelations = getCleanStateRelations(plan);
        List<SchemaTableName> naturalJoinTables = new ArrayList<>();
        for (SchemaTableName schemaTableName : joinOrdering.getSchemaTableNameList()) {
            if (!schemaTableName.equals(targetNode.getSchemaTableName())) {
                naturalJoinTables.add(schemaTableName);
            }
            else {
                break;
            }
        }
        SchemaTableName naturalJoinResult = new SchemaTableName(database.getSchemaName(),
                Joiner.on("J").join(naturalJoinTables.stream().map(schemaTableName -> schemaTableName.getTableName()).collect(Collectors.toList())));
        PostgresNaturalJoinExecutor postgresNaturalJoinExecutor = new PostgresNaturalJoinExecutor(database, naturalJoinResult.getTableName());
        postgresNaturalJoinExecutor.executeNaturalJoinOnPostgres(naturalJoinTables);
        List<MultiwayJoinNode> children = joinTree.getChildren().get(targetNode);
        List<MultiwayJoinNode> prevChild = new ArrayList<>();
        int semiJoinResultSizeSum = 0;
        for (MultiwayJoinNode child : children) {
            SchemaTableName antiJoinResultRelation = generateAntiJoinResultRelation(child, cleanStateRelations.get(child.getSchemaTableName()));
            int semiJoinResultSize = generateSemiJoinResult(targetNode, naturalJoinResult, prevChild, cleanStateRelations, antiJoinResultRelation).size();
            semiJoinResultSizeSum += semiJoinResultSize;
            prevChild.add(child);
        }
        return semiJoinResultSizeSum / targetRelationSize;
    }

    /**
     * Computes U \antijoin \widetilde(U) in (1) and G \antijoin \widetile(G) in (2)
     */
    private SchemaTableName generateAntiJoinResultRelation(MultiwayJoinNode childNode, SchemaTableName cleanStateRelation)
    {
        JoinOrdering joinOrdering1 = new JoinOrdering(List.of(childNode.getSchemaTableName(), cleanStateRelation));
        List<String> attributes = jdbcClient.getAttributes(cleanStateRelation);
        List<Type> types = jdbcClient.getColumns(jdbcClient.getTableHandle(cleanStateRelation)).stream().map(JdbcColumnHandle::getColumnType).collect(Collectors.toList());
        MultiwayJoinNode cleanStateNode = new MultiwayJoinNode(cleanStateRelation, attributes, null);
        MultiwayJoinOrderedGraph joinTree = constructOrderedGraph(childNode, List.of(cleanStateNode));
        Plan plan = createPhysicalPlanFromJoinOrdering(joinOrdering1, joinTree, TupleBasedTableScanOperator.class, TupleBasedLeftAntiHashJoinOperator.class);
        SchemaTableName ret = new SchemaTableName(database.getSchemaName(), childNode.getSchemaTableName().getTableName() + "_antijoin");
        List<Row> results = new ArrayList<>();
        Operator root = plan.getRoot().getOperator();
        root.open();
        while (true) {
            Row row = root.getNext();
            if (row == null) {
                root.close();
                jdbcClient.ingestRelation(ret.getSchemaName(),
                        ret.getTableName(),
                        attributes,
                        types,
                        results.stream().map(Row::getVals).collect(Collectors.toList()));
                break;
            }
            else {
                results.add(row);
            }
        }
        return ret;
    }

    /**
     * Computes T \leftsemijoin (R \join S) \leftsemijoin (G \antijoin \widetilde(G)) in (1) and
     * T \leftsemijoin (R \join S) \leftsemijoin \widetilde(U) \leftsemijoin (G \antijoin \widetilde(G)) in (2)
     */
    private List<Row> generateSemiJoinResult(MultiwayJoinNode targetNode,
                                             SchemaTableName naturalJoinResult,
                                             List<MultiwayJoinNode> prevChildren,
                                             Map<SchemaTableName, SchemaTableName> cleanStateRelations,
                                             SchemaTableName antiJoinResultRelation)
    {
        List<SchemaTableName> joinOrderSchema = new ArrayList<>();
        joinOrderSchema.add(targetNode.getSchemaTableName());
        joinOrderSchema.add(naturalJoinResult);
        for (MultiwayJoinNode prevChild : prevChildren) {
            joinOrderSchema.add(cleanStateRelations.get(prevChild.getSchemaTableName()));
        }
        joinOrderSchema.add(antiJoinResultRelation);
        JoinOrdering joinOrdering1 = new JoinOrdering(joinOrderSchema);
        List<MultiwayJoinNode> children = new ArrayList<>();
        for (int i = 1; i < joinOrderSchema.size(); ++i) {
            children.add(new MultiwayJoinNode(joinOrderSchema.get(i), jdbcClient.getAttributes(joinOrderSchema.get(i)), null));
        }
        MultiwayJoinOrderedGraph joinTree = constructOrderedGraph(targetNode, children);
        Plan plan = createPhysicalPlanFromJoinOrdering(joinOrdering1, joinTree, TupleBasedTableScanOperator.class, TupleBasedLeftSemiHashJoinOperator.class);
        List<Row> results = new ArrayList<>();
        Operator root = plan.getRoot().getOperator();
        root.open();
        while (true) {
            Row row = root.getNext();
            if (row == null) {
                root.close();
                return results;
            }
            else {
                results.add(row);
            }
        }
    }

    private Plan createPhysicalPlanFromJoinOrdering(JoinOrdering joinOrdering,
                                                    MultiwayJoinOrderedGraph joinTree,
                                                    Class<? extends TupleBasedTableScanOperator> tableScanOperatorClazz,
                                                    Class<? extends TupleBasedJoinOperator> joinOperatorClazz)
    {
        PlanBuildContext.Builder builder = builder();
        builder.setPlanNodeIdAllocator(planNodeIdAllocator)
                .setOperatorMap(createMap(Optional.of(tableScanOperatorClazz),
                        Optional.of(joinOperatorClazz)))
                .setJdbcClient(jdbcClient)
                .setOrderedGraph(joinTree)
                .setRules(List.of());
        PlanBuildContext context = builder.build();
        PlanBuilder planBuilder = new PlanBuilder(joinOrdering.getSchemaTableNameList(), context);
        Plan logicalPlan = planBuilder.build();
        RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
        return physicalPlanBuilder.build(logicalPlan.getRoot());
    }

    private Map<SchemaTableName, SchemaTableName> getCleanStateRelations(Plan plan)
    {
        Operator root = plan.getRoot().getOperator();
        root.open();
        while (true) {
            Row row = root.getNext();
            if (row == null) {
                Map<MultiwayJoinNode, List<Row>> relation2CleanState = HashTableVistior.gatherCleanStateRelations(plan.getRoot());
                root.close();
                return ingestRelations(relation2CleanState);
            }
        }
    }

    private Map<SchemaTableName, SchemaTableName> ingestRelations(Map<MultiwayJoinNode, List<Row>> relation2CleanState)
    {
        Map<SchemaTableName, SchemaTableName> ret = new HashMap<>();
        for (MultiwayJoinNode node : relation2CleanState.keySet()) {
            SchemaTableName schemaTableName = node.getSchemaTableName();
            JdbcTableHandle tableHandle = jdbcClient.getTableHandle(schemaTableName);
            List<Type> typeList = jdbcClient.getColumns(tableHandle).stream().map(JdbcColumnHandle::getColumnType).collect(Collectors.toList());
            String newRelationName = schemaTableName.getTableName() + "_clean";
            SchemaTableName newSchemaTableName = new SchemaTableName(database.getSchemaName(), newRelationName);
            ret.put(schemaTableName, newSchemaTableName);
            if (jdbcClient.getTableHandle(newSchemaTableName) == null) {
                List<Row> rows = relation2CleanState.get(node);
                jdbcClient.ingestRelation(newSchemaTableName.getSchemaName(),
                        newRelationName,
                        node.getAttributes(),
                        typeList,
                        rows.stream().map(Row::getVals).collect(Collectors.toList()));
            }
        }
        return ret;
    }

    /**
     * We gather all the hash tables from join operators
     */
    private static class HashTableVistior
            extends PlanVisitor<Void, Void>
    {
        private final Map<MultiwayJoinNode, List<Row>> relation2CleanState;

        public static Map<MultiwayJoinNode, List<Row>> gatherCleanStateRelations(PlanNode root)
        {
            return (new HashTableVistior(root)).relation2CleanState;
        }

        private HashTableVistior(PlanNode root)
        {
            relation2CleanState = new HashMap<>();
            visitPlan(root, null);
        }

        @Override
        public Void visitPlan(PlanNode node, Void context)
        {
            List<PlanNode> children = node.getSources();
            for (PlanNode child : children) {
                if (child != null) {
                    visitPlan(child, context);
                }
            }
            return node.accept(this, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            TupleBasedHashJoinOperator operator = (TupleBasedHashJoinOperator) node.getOperator();
            List<Row> rows = new ArrayList<>();
            for (List<Row> rowList : operator.getHashTableH().values()) {
                rows.addAll(rowList);
            }
            relation2CleanState.put(node.getRight().getOperator().getMultiwayJoinNode(), rows);
            node.getLeft().accept(this, context);
            return null;
        }

        @Override
        public Void visitTable(TableNode node, Void context)
        {
            return null;
        }

        @Override
        public Void visitFullReducer(FullReducerNode node, Void context)
        {
            return null;
        }
    }

    private static class CSJMDB
            extends TestingDatabase
    {
        JdbcClient jdbcClient;
        private final String schemaName = "csjmdb";

        public CSJMDB(JdbcClient jdbcClient)
        {
            this.jdbcClient = jdbcClient;
            try (Connection connection = jdbcClient.getConnection();
                    Statement statement = connection.createStatement()) {
                statement.execute(String.format("DROP SCHEMA IF EXISTS %s CASCADE", schemaName));
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
            }
            catch (SQLException e) {
                throw new TreeTrackerException(JDBC_ERROR, e);
            }
        }

        @Override
        public String getSchemaName()
        {
            return schemaName;
        }

        @Override
        public JdbcClient getJdbcClient()
        {
            return jdbcClient;
        }
    }

    public static HashMap<MultiwayJoinNode, Float> getSemiJoinModRatioResult(QueryEnum queryEnum)
    {
        System.out.println("query: " + queryEnum);
        JoinFragmentType query = queryProvider(JoinOperator.TTJHP, queryEnum,
                List.of(),
                postgresJdbcClientSupplier.get());
        MultiwayJoinOrderedGraph joinTree = query.getPlan().getRoot().getOperator().getPlanBuildContext().getOrderedGraph();
        JoinOrdering joinOrdering1 = getJoinOrderingFromNodes(joinTree.getTraversalList());
        CollectSemiJoinModRatio collectSemiJoinModRatio = new CollectSemiJoinModRatio(joinTree, joinOrdering1);
        HashMap<MultiwayJoinNode, Float> semiJoinModRatioResult1 = collectSemiJoinModRatio.getSemiJoinModRatioResult();
        System.out.println(semiJoinModRatioResult1);
        return semiJoinModRatioResult1;
    }

    public static void main(String[] args)
    {
        getSemiJoinModRatioResult(TPCHQueriesAutoGen.Query10WOptJoinTreeOptOrdering);
        getSemiJoinModRatioResult(TPCHQueriesAutoGen.Query7aWOptJoinTreeOptOrdering);
        getSemiJoinModRatioResult(TPCHQueriesAutoGen.Query7bWOptJoinTreeOptOrdering);
    }
}
