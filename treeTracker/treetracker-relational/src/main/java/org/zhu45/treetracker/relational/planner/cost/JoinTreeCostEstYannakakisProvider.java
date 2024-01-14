package org.zhu45.treetracker.relational.planner.cost;

import com.google.common.base.Joiner;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import de.renebergelt.test.Switches;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.NodeType;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcSupplier;
import org.zhu45.treetracker.jdbc.PostgreSqlClient;
import org.zhu45.treetracker.jdbc.QueryBuilder;
import org.zhu45.treetracker.relational.planner.PlanBuildContext;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;
import org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph.getSchemaTableNameList;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.zhu45.treetracker.relational.planner.cost.PostgresCardEstProvider.extractEstimateFromPlan;
import static org.zhu45.treetracker.relational.planner.rule.SemiJoinOrdering.integrityCheck;

/**
 * Provides the cost estimate of a join tree using SQL generation based on cost-model-y,
 * i.e., the cost of join tree under Yannakakis's algorithm evaluation
 */
public class JoinTreeCostEstYannakakisProvider
        implements JoinTreeCostProvider
{
    public static JoinTreeCostProviderConfiguration defaultConfiguration = JoinTreeCostProviderConfiguration.builder(JoinTreeCostProviderConfiguration.EstimationMethod.SQL)
            .enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult(true)
            .includeInnerRelationSize(true)
            .enableGreedySemijoinOrdering(true)
            .build();

    private static final Logger traceLogger;

    private final HashMap<String, Long> query2CostCache = new HashMap<>();
    private int numCacheHit;
    private int numSQLsGen;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(JoinTreeCostEstYannakakisProvider.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private final boolean useTrueCard;
    private final boolean enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
    private final boolean includeInnerRelationSize;
    private final boolean enableGreedySemijoinOrdering;

    public JoinTreeCostEstYannakakisProvider(JoinTreeCostProviderConfiguration config)
    {
        this.useTrueCard = config.isUseTrueCard();
        this.enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult = config.isEnableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult();
        this.includeInnerRelationSize = config.isIncludeInnerRelationCleanStateSize();
        this.enableGreedySemijoinOrdering = config.isEnableGreedySemijoinOrdering();
    }

    @Override
    public JoinTreeCostReturn getCost(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context)
    {
        return getCostSQL(ordering, joinTree);
    }

    @Override
    public boolean isUseTrueCard()
    {
        return useTrueCard;
    }

    private JoinTreeCostReturn getCostSQL(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree)
    {
        checkArgument(ordering.getSchemaTableNameList().equals(getSchemaTableNameList(joinTree)));
        return getCostEstimate(joinTree);
    }

    private JoinTreeCostReturn getCostEstimate(MultiwayJoinOrderedGraph joinTree)
    {
        double estimate = 0;
        double estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult = 0;
        double estimateInnerRelationCleanStateSize = 0;
        String currSQL = "";
        List<String> sqls = new ArrayList<>();
        if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
            traceLogger.debug("joinTree: " + joinTree);
        }
        PostgreSqlClient postgresJdbcClient = (PostgreSqlClient) JdbcSupplier.postgresJdbcClientSupplier.get();
        CostEstimateQueryBuilder costEstimateQueryBuilder =
                new CostEstimateQueryBuilder(postgresJdbcClient.getIdentifierQuote(), useTrueCard);
        int sizeOfOrdering = joinTree.getTraversalList().size();
        SemiJoinOrdering semiJoinOrdering = new SemiJoinOrdering(new ArrayList<>(), joinTree);
        for (int i = 0; i < sizeOfOrdering; ++i) {
            MultiwayJoinNode joinNode = joinTree.getTraversalList().get(i);
            switch (joinNode.getNodeType()) {
                case Leaf:
                    estimate += getEstimate(postgresJdbcClient, costEstimateQueryBuilder,
                            joinNode, null, List.of(), joinTree, sqls);
                    break;
                case Root:
                case Internal:
                    Pair<Double, SemiJoinOrdering> pair = enableGreedySemijoinOrdering ?
                            costBasedOnGreedySemijoinOrdering(joinNode, joinTree, postgresJdbcClient, costEstimateQueryBuilder, sqls) :
                            costBasedOnDefaultSemijoinOrdering(joinNode, joinTree, postgresJdbcClient, costEstimateQueryBuilder, sqls);
                    estimate += pair.getKey();
                    semiJoinOrdering.insertAtHead(pair.getRight());
                    break;
                default:
                    throw new RuntimeException("joinNode has NodeType: " + joinNode.getNodeType());
            }
            if (enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult) {
                Pair<Long, String> pair = getSizeOfIntermediateResultsThatArePartOfFinalJoinResult(postgresJdbcClient, costEstimateQueryBuilder, currSQL, sqls, joinNode, i, joinTree);
                currSQL = pair.getRight();
                estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult += pair.getLeft();
            }
            if (includeInnerRelationSize && joinNode.getNodeType() != NodeType.Root) {
                estimateInnerRelationCleanStateSize += getInnerRelationFullReducerStateSize(postgresJdbcClient, costEstimateQueryBuilder, sqls, joinNode, joinTree);
            }
        }
        estimate += estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        estimate += estimateInnerRelationCleanStateSize;
        integrityCheck(semiJoinOrdering);
        return JoinTreeCostReturn.builder(estimate)
                .setSql(sqls)
                .setCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult(estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult)
                .innerRelationCleanStateSize(estimateInnerRelationCleanStateSize)
                .setSemijoinOrdering(semiJoinOrdering)
                .build();
    }

    private Pair<Double, SemiJoinOrdering> costBasedOnDefaultSemijoinOrdering(MultiwayJoinNode joinNode,
                                                                              MultiwayJoinOrderedGraph joinTree,
                                                                              JdbcClient jdbcClient,
                                                                              JoinTreeCostEstYannakakisProvider.CostEstimateQueryBuilder costEstimateQueryBuilder,
                                                                              List<String> sqls)
    {
        checkArgument(joinNode.getNodeType() == NodeType.Internal ||
                        joinNode.getNodeType() == NodeType.Root,
                joinNode + "is neither internal nor root");
        List<MultiwayJoinNode> children = joinTree.getChildren().get(joinNode);
        List<MultiwayJoinNode> prevVisitedChildren = new ArrayList<>();
        double estimate = 0;
        List<Pair<MultiwayJoinNode, MultiwayJoinNode>> semijoins = new ArrayList<>();
        for (MultiwayJoinNode child : children) {
            estimate += getEstimate(jdbcClient, costEstimateQueryBuilder, joinNode, child, prevVisitedChildren, joinTree, sqls);
            prevVisitedChildren.add(child);
            semijoins.add(Pair.of(joinNode, child));
        }
        return Pair.of(estimate, new SemiJoinOrdering(semijoins, joinTree));
    }

    private Pair<Double, SemiJoinOrdering> costBasedOnGreedySemijoinOrdering(MultiwayJoinNode joinNode,
                                                                             MultiwayJoinOrderedGraph joinTree,
                                                                             JdbcClient jdbcClient,
                                                                             JoinTreeCostEstYannakakisProvider.CostEstimateQueryBuilder costEstimateQueryBuilder,
                                                                             List<String> sqls)
    {
        checkArgument(joinNode.getNodeType() == NodeType.Internal ||
                        joinNode.getNodeType() == NodeType.Root,
                joinNode + "is neither internal nor root");
        List<MultiwayJoinNode> children = joinTree.getChildren().get(joinNode);
        List<MultiwayJoinNode> prevVisitedChildren = new ArrayList<>();
        double estimate = 0;
        List<Pair<MultiwayJoinNode, MultiwayJoinNode>> semijoins = new ArrayList<>();
        for (int i = 0; i < children.size(); ++i) {
            PriorityQueue<Pair<MultiwayJoinNode, Double>> pq = new PriorityQueue<>(Comparator.comparingDouble(Pair::getRight));
            for (MultiwayJoinNode child : children) {
                if (!prevVisitedChildren.contains(child)) {
                    // For internal node, the estimate also contains |R_i^*|, which is doesn't matter for our purpose because
                    // the value is agnostic to the semijoin ordering. Also, since \widetilde{R} is agnostic to the ordering
                    // of semijoins as well, there is no recursion meaning for a node and two subtrees, we don't need to
                    // recursion inside subtrees to determine the semijoin ordering.
                    pq.add(Pair.of(child, getEstimate(jdbcClient, costEstimateQueryBuilder, joinNode, child, prevVisitedChildren, joinTree, sqls)));
                }
            }
            checkState(!pq.isEmpty());
            Pair<MultiwayJoinNode, Double> minCostPair = pq.poll();
            prevVisitedChildren.add(minCostPair.getLeft());
            estimate += minCostPair.getRight();
            semijoins.add(Pair.of(joinNode, minCostPair.getLeft()));
        }
        return Pair.of(estimate, new SemiJoinOrdering(semijoins, joinTree));
    }

    private double getEstimate(JdbcClient jdbcClient,
                               JoinTreeCostEstYannakakisProvider.CostEstimateQueryBuilder costEstimateQueryBuilder,
                               MultiwayJoinNode joinNode,
                               MultiwayJoinNode child,
                               List<MultiwayJoinNode> prevVisitedChildren,
                               MultiwayJoinOrderedGraph joinTree,
                               List<String> sqls)
    {
        long estimateFromPlan = 0;
        try (Connection connection = jdbcClient.getConnection();
                PreparedStatement preparedStatement = costEstimateQueryBuilder
                        .buildCostSql(jdbcClient, connection, joinNode,
                                child, prevVisitedChildren, joinTree)) {
            numSQLsGen++;
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug("joinNode: " + joinNode);
                traceLogger.debug("child: " + child);
                traceLogger.debug("preparedStatement: " + preparedStatement.toString());
            }
            String sql = preparedStatement.toString();
            sqls.add(sql);
            if (query2CostCache.containsKey(sql)) {
                numCacheHit++;
                estimateFromPlan = query2CostCache.get(sql);
            }
            else {
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    resultSet.next();
                    String rawJsonString = resultSet.getString(1);
                    JsonArray obj = JsonParser.parseString(rawJsonString).getAsJsonArray();
                    estimateFromPlan = extractEstimateFromPlan(obj, useTrueCard);
                    query2CostCache.put(sql, estimateFromPlan);
                }
            }
            return estimateFromPlan;
        }
        catch (SQLException e) {
            throw new TreeTrackerException(JDBC_ERROR, e);
        }
    }

    public Pair<Long, String> getSizeOfIntermediateResultsThatArePartOfFinalJoinResult(JdbcClient client,
                                                                                       CostEstimateQueryBuilder costEstimateQueryBuilder,
                                                                                       String currSQL,
                                                                                       List<String> sqls,
                                                                                       MultiwayJoinNode targetNode,
                                                                                       int targetNodePositionInOrdering,
                                                                                       MultiwayJoinOrderedGraph joinTree)
    {
        try (Connection connection = client.getConnection()) {
            String sql = costEstimateQueryBuilder.buildSizeOfIntermediateResultsThatArePartOfFinalJoinResultSql(targetNode, targetNodePositionInOrdering, joinTree, connection.getCatalog(), currSQL);
            numSQLsGen++;
            if (query2CostCache.containsKey(sql)) {
                numCacheHit++;
                sqls.add(costEstimateQueryBuilder.appendExplainHeader(sql));
                return Pair.of(query2CostCache.get(sql), sql);
            }
            else {
                try (PreparedStatement preparedStatement = client.getPreparedStatement(connection, costEstimateQueryBuilder.appendExplainHeader(sql))) {
                    if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                        traceLogger.debug("preparedStatement (on inter. size): " + preparedStatement.toString());
                    }
                    sqls.add(preparedStatement.toString());
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        resultSet.next();
                        String rawJsonString = resultSet.getString(1);
                        JsonArray obj = JsonParser.parseString(rawJsonString).getAsJsonArray();
                        long estimate = extractEstimateFromPlan(obj, useTrueCard);
                        query2CostCache.put(sql, estimate);
                        return Pair.of(estimate, sql);
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long getInnerRelationFullReducerStateSize(JdbcClient client,
                                                     CostEstimateQueryBuilder costEstimateQueryBuilder,
                                                     List<String> sqls,
                                                     MultiwayJoinNode targetNode,
                                                     MultiwayJoinOrderedGraph joinTree)
    {
        try (Connection connection = client.getConnection()) {
            String sql = costEstimateQueryBuilder.buildRStar(targetNode.getAttributes(), connection.getCatalog(), targetNode, joinTree);
            numSQLsGen++;
            if (query2CostCache.containsKey(sql)) {
                numCacheHit++;
                sqls.add(costEstimateQueryBuilder.appendExplainHeader(sql));
                return query2CostCache.get(sql);
            }
            else {
                try (PreparedStatement preparedStatement = client.getPreparedStatement(connection, costEstimateQueryBuilder.appendExplainHeader(sql))) {
                    if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                        traceLogger.debug("preparedStatement (on inner relation clean state size): " + preparedStatement.toString());
                    }
                    sqls.add(preparedStatement.toString());
                    try (ResultSet resultSet = preparedStatement.executeQuery()) {
                        resultSet.next();
                        String rawJsonString = resultSet.getString(1);
                        JsonArray obj = JsonParser.parseString(rawJsonString).getAsJsonArray();
                        long estimate = extractEstimateFromPlan(obj, useTrueCard);
                        query2CostCache.put(sql, estimate);
                        return estimate;
                    }
                }
            }
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected static class CostEstimateQueryBuilder
            extends QueryBuilder
    {
        private final String explainHeader;
        private final CostEstimateQueryBuilder.AliasAllocator aliasAllocator;

        public CostEstimateQueryBuilder(String quote, boolean useTrueCard)
        {
            super(quote);
            if (useTrueCard) {
                explainHeader = "EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON, ANALYZE) ";
            }
            else {
                explainHeader = "EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON) ";
            }
            aliasAllocator = new CostEstimateQueryBuilder.AliasAllocator();
        }

        /**
         * Build R^* in full reducer
         */
        public String buildRStar(List<String> selectedColumns,
                                 String catalog,
                                 MultiwayJoinNode target,
                                 MultiwayJoinOrderedGraph joinTree)
        {
            checkArgument(target.getNodeType() != NodeType.None,
                    target + "has to have a node type (cannot be None)");
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ")
                    .append(Joiner.on(",").join(selectedColumns))
                    .append(" FROM ")
                    .append(getFullQualifiedTable(catalog, target.getSchemaTableName()))
                    .append(" WHERE ");
            if (target.getNodeType() == NodeType.Leaf) {
                MultiwayJoinNode parent = joinTree.getParent().get(target).get(0);
                List<String> sharedAttributes = sharedAttributes(parent, target);
                String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                StringBuilder whereCondition = new StringBuilder();
                whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                whereCondition.append(" IN ");
                whereCondition.append(String.format("(%s)", buildRStar(sharedAttributes, catalog, parent, joinTree)));
                sql.append(whereCondition);
                return sql.toString();
            }
            else if (target.getNodeType() == NodeType.Root) {
                List<MultiwayJoinNode> children = joinTree.getChildren().get(target);
                List<String> whereConditions = new ArrayList<>();
                for (MultiwayJoinNode child : children) {
                    List<String> sharedAttributes = sharedAttributes(target, child);
                    String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                    StringBuilder whereCondition = new StringBuilder();
                    whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                    whereCondition.append(" IN ");
                    whereCondition.append(String.format("(%s)", buildWidetildeR(sharedAttributes, catalog,
                            child, joinTree)));
                    whereConditions.add(whereCondition.toString());
                }
                sql.append(Joiner.on(" AND ").join(whereConditions));
                return sql.toString();
            }
            else {
                List<MultiwayJoinNode> children = joinTree.getChildren().get(target);
                List<String> whereConditions = new ArrayList<>();
                for (MultiwayJoinNode child : children) {
                    List<String> sharedAttributes = sharedAttributes(target, child);
                    String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                    StringBuilder whereCondition = new StringBuilder();
                    whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                    whereCondition.append(" IN ");
                    whereCondition.append(String.format("(%s)", buildWidetildeR(sharedAttributes, catalog,
                            child, joinTree)));
                    whereConditions.add(whereCondition.toString());
                }
                MultiwayJoinNode parent = joinTree.getParent().get(target).get(0);
                List<String> sharedAttributes = sharedAttributes(parent, target);
                String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                StringBuilder whereCondition = new StringBuilder();
                whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                whereCondition.append(" IN ");
                whereCondition.append(String.format("(%s)", buildRStar(sharedAttributes, catalog, parent, joinTree)));
                whereConditions.add(whereCondition.toString());
                sql.append(Joiner.on(" AND ").join(whereConditions));
                return sql.toString();
            }
        }

        /**
         * Build \widetilde{R}, the content of relations after bottom-pass of the join tree, i.e., after running
         * reducing semijoin program.
         */
        public String buildWidetildeR(List<String> selectedColumns,
                                      String catalog,
                                      MultiwayJoinNode targetNode,
                                      MultiwayJoinOrderedGraph joinTree)
        {
            checkArgument(targetNode.getNodeType() != NodeType.None,
                    targetNode + " cannot have None type");
            checkArgument(targetNode.getNodeType() != NodeType.Root,
                    targetNode + " cannot be Root per the definition of \\widetilde{\\phyR}");
            StringBuilder sql = new StringBuilder();
            if (targetNode.getNodeType() == NodeType.Leaf) {
                sql.append("SELECT ")
                        .append(Joiner.on(",").join(selectedColumns))
                        .append(" FROM ")
                        .append(getFullQualifiedTable(catalog, targetNode.getSchemaTableName()));
                return sql.toString();
            }
            else {
                List<MultiwayJoinNode> children = joinTree.getChildren().get(targetNode);
                List<String> whereConditions = new ArrayList<>();
                for (MultiwayJoinNode child : children) {
                    List<String> sharedAttributes = sharedAttributes(targetNode, child);
                    String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                    StringBuilder whereCondition = new StringBuilder();
                    whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                    whereCondition.append(" IN ");
                    whereCondition.append(String.format("(%s)", buildWidetildeR(sharedAttributes, catalog,
                            child, joinTree)));
                    whereConditions.add(whereCondition.toString());
                }
                sql.append(" SELECT ")
                        .append(Joiner.on(",").join(selectedColumns));
                sql.append(" FROM ")
                        .append(getFullQualifiedTable(catalog, targetNode.getSchemaTableName()))
                        .append(" WHERE ")
                        .append(Joiner.on("AND ").join(whereConditions));
                return sql.toString();
            }
        }

        public PreparedStatement buildCostSql(JdbcClient client,
                                              Connection connection,
                                              MultiwayJoinNode parent,
                                              MultiwayJoinNode child,
                                              List<MultiwayJoinNode> prevVisitedChildren,
                                              MultiwayJoinOrderedGraph joinTree)
                throws SQLException
        {
            String catalog = connection.getCatalog();
            switch (parent.getNodeType()) {
                case Root:
                    return client.getPreparedStatement(connection, buildCostSqlRoot(joinTree, catalog, prevVisitedChildren, child));
                case Leaf:
                    return client.getPreparedStatement(connection, buildCostSqlLeaf(catalog,
                            true, parent.getAttributes(), parent, joinTree));
                case Internal:
                    return client.getPreparedStatement(connection, buildCostSqlInternal(catalog,
                            true, parent.getAttributes(), parent, child, prevVisitedChildren, joinTree));
                default:
                    throw new RuntimeException(parent + "has NONE NodeType");
            }
        }

        public String buildSizeOfIntermediateResultsThatArePartOfFinalJoinResultSql(MultiwayJoinNode targetNode,
                                                                                    int targetNodePositionInOrdering,
                                                                                    MultiwayJoinOrderedGraph joinTree,
                                                                                    String catalog,
                                                                                    String currSQL)
                throws SQLException
        {
            if (targetNodePositionInOrdering == 0) {
                return buildRStar(targetNode.getAttributes(), catalog, targetNode, joinTree);
            }
            else if (targetNodePositionInOrdering == joinTree.getTraversalList().size() - 1) {
                // For the last relation in the ordering, we can directly return the natural join query, which is equivalent
                // to the sql generated few lines below in this method but using natural join query may improve estimation accuracy.
                return requireNonNull(getNaturalJoinQuery(catalog, targetNode, true, joinTree)).getRight();
            }
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT * FROM ")
                    .append("(")
                    .append(currSQL)
                    .append(") AS ")
                    .append(aliasAllocator.getNextAlias())
                    .append(" NATURAL JOIN ")
                    .append(String.format("(%s) AS %s",
                            buildRStar(targetNode.getAttributes(), catalog, targetNode, joinTree),
                            aliasAllocator.getNextAlias()));
            return sql.toString();
        }

        public String appendExplainHeader(String sql)
        {
            StringBuilder builder = new StringBuilder(explainHeader);
            return builder.append(sql).toString();
        }

        private String buildCostSqlRoot(MultiwayJoinOrderedGraph joinTree,
                                        String catalog,
                                        List<MultiwayJoinNode> prevVisitedChildren,
                                        MultiwayJoinNode child)
        {
            StringBuilder sql = new StringBuilder(explainHeader);
            sql.append(buildTildeMathBBRuSqlOrNegate(joinTree.getRoot().getAttributes(),
                    catalog,
                    joinTree.getRoot(),
                    List.of(child),
                    prevVisitedChildren,
                    joinTree));
            return sql.toString();
        }

        private String buildCostSqlLeaf(String catalog,
                                        boolean enableExplainHeader,
                                        List<String> selectedColumns,
                                        MultiwayJoinNode parent,
                                        MultiwayJoinOrderedGraph joinTree)
        {
            StringBuilder sql = new StringBuilder();
            if (enableExplainHeader) {
                sql.append(explainHeader);
            }
            sql.append(buildRStar(selectedColumns, catalog, parent, joinTree));
            return sql.toString();
        }

        private String buildCostSqlInternal(String catalog,
                                            boolean enableExplainHeader,
                                            List<String> selectedColumns,
                                            MultiwayJoinNode parent,
                                            MultiwayJoinNode child,
                                            List<MultiwayJoinNode> prevVisitedChildren,
                                            MultiwayJoinOrderedGraph joinTree)
        {
            StringBuilder sql = new StringBuilder();
            if (enableExplainHeader) {
                sql.append(explainHeader);
            }
            sql.append(buildTildeMathBBRuSqlOrNegate(selectedColumns,
                    catalog,
                    parent,
                    List.of(child),
                    prevVisitedChildren,
                    joinTree));
            if (prevVisitedChildren.isEmpty()) {
                sql.append(" UNION ALL ")
                        .append(buildRStar(selectedColumns, catalog, parent, joinTree));
            }
            return sql.toString();
        }

        /**
         * Implement \mathbb{R}_i^{[t]} \leftsemijoin \widetilde{\mathbb{R}}_u^{t+1} where
         * \mathbb{R}_i can be either internal node or root node of the join tree.
         */
        private String buildTildeMathBBRuSqlOrNegate(List<String> selectedColumns,
                                                     String catalog,
                                                     MultiwayJoinNode targetNode,
                                                     List<MultiwayJoinNode> childNodes,
                                                     List<MultiwayJoinNode> prevVisitedChildren,
                                                     MultiwayJoinOrderedGraph joinTree)
        {
            checkArgument(targetNode.getNodeType() != NodeType.None, targetNode + "has NodeType.None");
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug("targetNode: " + targetNode);
                traceLogger.debug("targetNode nodeType: " + targetNode.getNodeType());
                traceLogger.debug("childNodes: " + childNodes);
            }
            String inOrNotInOperator = "IN";
            StringBuilder builder = new StringBuilder();
            if (targetNode.getNodeType() == NodeType.Leaf) {
                builder.append("SELECT ")
                        .append(Joiner.on(",").join(selectedColumns))
                        .append(" FROM ")
                        .append(buildFromClause(selectedColumns, catalog, targetNode, prevVisitedChildren, joinTree));
                return builder.toString();
            }
            List<String> whereClauses = new ArrayList<>();
            for (MultiwayJoinNode child : childNodes) {
                List<String> sharedAttributes = sharedAttributes(targetNode, child);
                checkState(!sharedAttributes.isEmpty(),
                        "sharedAttributes from " + targetNode + " and " + child + " cannot be empty");
                whereClauses.add("(" + Joiner.on(",").join(sharedAttributes)
                        + ") "
                        + inOrNotInOperator
                        + " ("
                        + buildWidetildeR(sharedAttributes, catalog, child, joinTree)
                        + ")");
            }

            builder.append(" SELECT ")
                    .append(Joiner.on(",").join(selectedColumns));
            builder.append(" FROM ")
                    .append(buildFromClause(selectedColumns, catalog, targetNode, prevVisitedChildren, joinTree))
                    .append(" WHERE ")
                    .append(Joiner.on("AND ").join(whereClauses));
            return builder.toString();
        }

        private List<String> sharedAttributes(MultiwayJoinNode node1, MultiwayJoinNode node2)
        {
            List<String> sharedAttributes = new ArrayList<>(node1.getAttributes());
            sharedAttributes.retainAll(node2.getAttributes());
            return sharedAttributes;
        }

        /**
         * Generates \mathbb{R}_i^{[t]}.
         */
        private String buildFromClause(List<String> selectedColumns,
                                       String catalog,
                                       MultiwayJoinNode targetNode,
                                       List<MultiwayJoinNode> prevVisitedChildren,
                                       MultiwayJoinOrderedGraph joinTree)
        {
            if (prevVisitedChildren.isEmpty()) {
                return getFullQualifiedTable(catalog, targetNode.getSchemaTableName());
            }
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("(SELECT ")
                    .append(Joiner.on(",").join(selectedColumns))
                    .append(" FROM ")
                    .append(getFullQualifiedTable(catalog, targetNode.getSchemaTableName()));
            List<String> whereConditions = new ArrayList<>();
            for (MultiwayJoinNode prevChild : prevVisitedChildren) {
                List<String> sharedAttributes = sharedAttributes(targetNode, prevChild);
                String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                StringBuilder whereCondition = new StringBuilder();
                whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                whereCondition.append(" IN ");
                whereCondition.append(String.format("(%s)", buildWidetildeR(sharedAttributes, catalog,
                        prevChild, joinTree)));
                whereConditions.add(whereCondition.toString());
            }
            stringBuilder.append(" WHERE ")
                    .append(Joiner.on("AND ").join(whereConditions))
                    .append(")")
                    .append(" AS ")
                    .append(aliasAllocator.getNextAlias());
            return stringBuilder.toString();
        }

        private String getFullQualifiedTable(String catalog, SchemaTableName schemaTableName)
        {
            StringBuilder sql = new StringBuilder();
            if (!isNullOrEmpty(catalog)) {
                sql.append(quote(catalog)).append('.');
            }
            sql.append(quote(schemaTableName.getSchemaName()))
                    .append('.')
                    .append(quote(schemaTableName.getTableName()));
            return sql.toString();
        }

        /**
         * The function generate the natural join query. Return is a pair
         * (selectedColumns, natural join query)
         * <p>
         * Take JOB Q1a as an example, the join tree is
         * <p>
         * <pre>
         * {@code
         * imdb.q1a_info_type(info_type_id)
         * |imdb_int.movie_info_idx(movie_id,info_type_id)
         * ||imdb.q1a_movie_companies(movie_id,company_id,company_type_id)
         * |||imdb.q1a_company_type(company_type_id)
         * |||imdb_int.title(movie_id,kind_id)
         * }
         * </pre>
         * and the join ordering is info_type, movie_info_idx, movie_companies, company_type, title
         * Then, given targetNode movie_companies, the function generates SQL
         * <pre>
         * {@code
         * SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx
         * }
         * </pre>
         * with selected columns movie_id
         */
        private Pair<List<String>, String> getNaturalJoinQuery(String catalog,
                                                               MultiwayJoinNode targetNode,
                                                               boolean includingTargetNode,
                                                               MultiwayJoinOrderedGraph joinTree)
        {
            if (targetNode.getNodeType() == NodeType.Root) {
                return null;
            }
            List<MultiwayJoinNode> naturalJoinRelations = new ArrayList<>();
            // TODO: check when comes to implement join ordering
            List<MultiwayJoinNode> ordering = joinTree.getTraversalList();
            for (MultiwayJoinNode node : ordering) {
                if (node.equals(targetNode)) {
                    if (includingTargetNode) {
                        naturalJoinRelations.add(targetNode);
                    }
                    break;
                }
                else {
                    naturalJoinRelations.add(node);
                }
            }

            List<String> fullQualifiedTables = new ArrayList<>();
            Set<String> naturalJoinRelationsAttributes = new HashSet<>();
            for (MultiwayJoinNode naturalJoinNode : naturalJoinRelations) {
                fullQualifiedTables.add(getFullQualifiedTable(catalog, naturalJoinNode.getSchemaTableName()));
                naturalJoinRelationsAttributes.addAll(requireNonNull(naturalJoinNode.getAttributes()));
            }

            List<String> targetNodeAttributes = requireNonNull(targetNode.getAttributes());
            List<String> sharedAttributes = new ArrayList<>();
            for (String targetNodeAttribute : targetNodeAttributes) {
                if (naturalJoinRelationsAttributes.contains(targetNodeAttribute)) {
                    sharedAttributes.add(targetNodeAttribute);
                }
            }
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ")
                    .append(Joiner.on(",").join(sharedAttributes))
                    .append(" FROM ")
                    .append(Joiner.on("natural join").join(fullQualifiedTables));
            return Pair.of(sharedAttributes, sql.toString());
        }

        public static class AliasAllocator
        {
            private int nextId;

            public String getNextAlias()
            {
                return "alias" + nextId++;
            }
        }
    }

    @Override
    public JoinTreeCostStatistics getStatistics()
    {
        return JoinTreeCostStatistics.builder(this.getClass())
                .cacheHit(numCacheHit)
                .numSQLsGen(numSQLsGen)
                .build();
    }
}
