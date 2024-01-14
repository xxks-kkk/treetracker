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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph.getSchemaTableNameList;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.jdbc.JdbcErrorCode.JDBC_ERROR;
import static org.zhu45.treetracker.relational.planner.cost.PostgresCardEstProvider.extractEstimateFromPlan;

/**
 * Provides the cost estimate of a join tree using SQL generation based on cost-model6
 * <p>
 * (probably we can instead implement cost model as input to this and have methods associate
 * with cost model to provide different ways to compute it, e.g., SQL generation
 * or catalog method).
 */
public class JoinTreeCostEstProvider
        implements JoinTreeCostProvider
{
    private static final Logger traceLogger;

    private final HashMap<String, Long> query2CostCache = new HashMap<>();
    private int numCacheHit;
    private int numSQLsGen;
    private int numJoinTreeCosted;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(JoinTreeCostEstProvider.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private final JoinTreeCostProviderConfiguration.EstimationMethod estimationMethod;
    private final boolean useTrueCard;
    private final boolean enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
    private final boolean includeInnerRelationSize;

    public JoinTreeCostEstProvider(JoinTreeCostProviderConfiguration config)
    {
        this.estimationMethod = config.getEstimationMethod();
        this.useTrueCard = config.isUseTrueCard();
        this.enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult = config.isEnableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult();
        this.includeInnerRelationSize = config.isIncludeInnerRelationCleanStateSize();
    }

    @Override
    public JoinTreeCostReturn getCost(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree, PlanBuildContext context)
    {
        numJoinTreeCosted++;
        // The model we implement is cost-model5-2 that contains both backjumping distance and the size of dangling
        // tuples. In the future, semantic optimization on whether to use no-good list or not can also be encoded.
        if (requireNonNull(estimationMethod) == JoinTreeCostProviderConfiguration.EstimationMethod.SQL) {
            return getCostSQL(ordering, joinTree);
        }
        throw new RuntimeException();
    }

    @Override
    public boolean isUseTrueCard()
    {
        return useTrueCard;
    }

    private JoinTreeCostReturn getCostSQL(JoinOrdering ordering, MultiwayJoinOrderedGraph joinTree)
    {
        checkArgument(ordering.getSchemaTableNameList().equals(getSchemaTableNameList(joinTree)));
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr = generateDSR(joinTree);
        return getCostEstimate(joinTree, dsr);
    }

    /**
     * Compute d_R^S for all nodes in the graph. The result is a map with key denote a node and value is
     * a list containing Map<child node, the path length>. The path length is the number of nodes between the node (including)
     * and child node (excluding) in the given join ordering.
     */
    private HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> generateDSR(
            MultiwayJoinOrderedGraph orderedGraph)
    {
        HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> result = new HashMap<>();
        for (int i = 0; i < orderedGraph.getTraversalList().size(); ++i) {
            MultiwayJoinNode node = orderedGraph.getTraversalList().get(i);
            result.put(node, new HashMap<>());
            List<MultiwayJoinNode> children = orderedGraph.getChildren().get(node);
            for (MultiwayJoinNode child : children) {
                result.get(node).put(child, findDistance(orderedGraph.getTraversalList(), i, child));
            }
        }
        return result;
    }

    /**
     * Obtain the distance between the node represented by startToCheckIdx (including) and the target (excluding)
     * in the ordering. For example, for the ordering V,S,T,U,R, the distance between S and U is 2 because
     * the startToCheckIdx is 1 (for S) and the target is U.
     */
    private int findDistance(List<MultiwayJoinNode> traversalList, int startToCheckIdx, MultiwayJoinNode target)
    {
        for (int i = startToCheckIdx; i < traversalList.size(); ++i) {
            if (traversalList.get(i).equals(target)) {
                return i - startToCheckIdx;
            }
        }
        throw new TreeTrackerException(GENERIC_INTERNAL_ERROR,
                String.format("the ordering represented by %s is wrong because %s (child of %s) doesn't appear after %s in the ordering",
                        traversalList,
                        target,
                        traversalList.get(startToCheckIdx),
                        traversalList.get(startToCheckIdx)));
    }

    private JoinTreeCostReturn getCostEstimate(MultiwayJoinOrderedGraph joinTree,
                                               HashMap<MultiwayJoinNode, HashMap<MultiwayJoinNode, Integer>> dsr)
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
        for (int i = 0; i < sizeOfOrdering; ++i) {
            MultiwayJoinNode joinNode = joinTree.getTraversalList().get(i);
            List<MultiwayJoinNode> children = joinTree.getChildren().get(joinNode);
            List<MultiwayJoinNode> prevVisitedChildren = new ArrayList<>();
            for (MultiwayJoinNode child : children) {
                try (Connection connection = postgresJdbcClient.getConnection();
                        PreparedStatement preparedStatement = costEstimateQueryBuilder
                                .buildCostSql(postgresJdbcClient, connection, joinNode,
                                        child, prevVisitedChildren, joinTree)) {
                    numSQLsGen++;
                    if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                        traceLogger.debug("joinNode: " + joinNode);
                        traceLogger.debug("child: " + child);
                        traceLogger.debug("preparedStatement: " + preparedStatement.toString());
                    }
                    String sql = preparedStatement.toString();
                    sqls.add(sql);
                    long estimateFromPlan = 0;
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
                    estimate += dsr.get(joinNode).get(child) * estimateFromPlan;
                    prevVisitedChildren.add(child);
                }
                catch (SQLException e) {
                    throw new TreeTrackerException(JDBC_ERROR, e);
                }
            }
            if (enableEstimateOnSizeOfIntermediateResultsThatArePartOfFinalJoinResult) {
                Pair<Long, String> pair = getSizeOfIntermediateResultsThatArePartOfFinalJoinResult(postgresJdbcClient, costEstimateQueryBuilder, currSQL, sqls, joinNode, i, joinTree);
                currSQL = pair.getRight();
                estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult += pair.getLeft();
            }
            if (includeInnerRelationSize && joinNode.getNodeType() != NodeType.Root) {
                estimateInnerRelationCleanStateSize += getInnerRelationCleanStateSize(postgresJdbcClient, costEstimateQueryBuilder, sqls, joinNode, joinTree);
            }
        }
        estimate += estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult;
        estimate += estimateInnerRelationCleanStateSize;
        return JoinTreeCostReturn.builder(estimate)
                .setSql(sqls)
                .setCostSizeOfIntermediateResultsThatArePartOfFinalJoinResult(estimateSizeOfIntermediateResultsThatArePartOfFinalJoinResult)
                .innerRelationCleanStateSize(estimateInnerRelationCleanStateSize)
                .build();
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

    public long getInnerRelationCleanStateSize(JdbcClient client,
                                               CostEstimateQueryBuilder costEstimateQueryBuilder,
                                               List<String> sqls,
                                               MultiwayJoinNode targetNode,
                                               MultiwayJoinOrderedGraph joinTree)
    {
        try (Connection connection = client.getConnection()) {
            String sql = costEstimateQueryBuilder.buildTildeMathBBRuSql(targetNode.getAttributes(), connection.getCatalog(), targetNode, joinTree);
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
        private final AliasAllocator aliasAllocator;

        public CostEstimateQueryBuilder(String quote, boolean useTrueCard)
        {
            super(quote);
            if (useTrueCard) {
                explainHeader = "EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON, ANALYZE) ";
            }
            else {
                explainHeader = "EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON) ";
            }
            aliasAllocator = new AliasAllocator();
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
            if (parent.getNodeType() != NodeType.Root) {
                return client.getPreparedStatement(connection, buildCostSqlNonRoot(catalog,
                        true, parent.getAttributes(), parent, child, prevVisitedChildren, joinTree));
            }
            return client.getPreparedStatement(connection, buildCostSqlRoot(joinTree, catalog, prevVisitedChildren, child));
        }

        public String buildSizeOfIntermediateResultsThatArePartOfFinalJoinResultSql(MultiwayJoinNode targetNode,
                                                                                    int targetNodePositionInOrdering,
                                                                                    MultiwayJoinOrderedGraph joinTree,
                                                                                    String catalog,
                                                                                    String currSQL)
                throws SQLException
        {
            if (targetNode.getNodeType() == NodeType.Root) {
                return buildRkStar(catalog, joinTree);
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
                            buildTildeMathBBRuSql(targetNode.getAttributes(), catalog, targetNode, joinTree),
                            aliasAllocator.getNextAlias()));
            return sql.toString();
        }

        public String appendExplainHeader(String sql)
        {
            StringBuilder builder = new StringBuilder(explainHeader);
            return builder.append(sql).toString();
        }

        public String buildRkStar(String catalog,
                                  MultiwayJoinOrderedGraph joinTree)
        {
            StringBuilder sql = new StringBuilder();
            MultiwayJoinNode root = joinTree.getRoot();
            sql.append("SELECT ")
                    .append(Joiner.on(",").join(root.getAttributes()))
                    .append(" FROM ")
                    .append(getFullQualifiedTable(catalog, root.getSchemaTableName()))
                    .append(" WHERE ");
            List<MultiwayJoinNode> children = joinTree.getChildren().get(root);
            List<String> whereConditions = new ArrayList<>();
            for (MultiwayJoinNode child : children) {
                List<String> sharedAttributes = sharedAttributes(root, child);
                String joinedSharedAttributes = Joiner.on(",").join(sharedAttributes);
                StringBuilder whereCondition = new StringBuilder();
                whereCondition.append(String.format("(%s)", joinedSharedAttributes));
                whereCondition.append(" IN ");
                whereCondition.append(String.format("(%s)", buildTildeMathBBRuSql(sharedAttributes, catalog, child, joinTree)));
                whereConditions.add(whereCondition.toString());
            }
            sql.append(Joiner.on(" AND ").join(whereConditions));
            return sql.toString();
        }

        /**
         * Provide cost estimation to the 3rd term of the cost equation (mostly correct; the exact version see note at
         * the end):
         * <ul><li>\sum_{\text{$R_k$'s children $S$}} |\dup(\project_{ja(R_k,S)} (\mathbb{R}_k \antijoin \tilde{\mathbb{S}}))|</li></ul>
         * Take JOB Q1a as an example, the join tree is
         * <pre>
         * {@code
         * imdb.q1a_info_type(info_type_id)
         * |imdb_int.movie_info_idx(movie_id,info_type_id)
         * ||imdb.q1a_movie_companies(movie_id,company_id,company_type_id)
         * |||imdb.q1a_company_type(company_type_id)
         * |||imdb_int.title(movie_id,kind_id)
         * }
         * </pre>
         * The root is q1a_info_type. It has only one child, imdb_int.movie_info_idx. The generated SQL is
         * <pre>
         * {@code
         * SELECT distinct(info_type_id)
         * FROM imdb.q1a_info_type
         * WHERE info_type_id not in (SELECT info_type_id
         *                              FROM imdb_int.movie_info_idx
         *                             WHERE info_type_id in (SELECT info_type_id from imdb.q1a_info_type)
         *                               AND movie_id in (SELECT movie_id from imdb.q1a_movie_companies
         *                                                 WHERE movie_id in (SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx)
         *                                                   AND company_type_id in (SELECT company_type_id from imdb.q1a_company_type)
         *                                                   AND movie_id in (SELECT movie_id from imdb_int.title));
         * }
         * </pre>
         * Note that the above cost equation is not perfect exact because \mathbb{R}_k has to be incrementally reduced
         * during the estimation, i.e., \mathbb{R}_k only appears in the generated SQL between \mathbb{R}_k and its first
         * child. For the rest generated SQLs involving the rest of children, \mathbb{R}_k is replaced by \phyR_i^{[t]}
         * (see cost-model6). However, above example is still correct with this extra note.
         */
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
                    joinTree,
                    true,
                    true));
            return sql.toString();
        }

        /**
         * This function provides cost estimation (mostly correct; the exact version see note at the end) to either term 1:
         * <ul><li>|\mathbb{R}_i \semijoin J_{i+1}^* \antijoin \mathbb{R}_u|
         * where \mathbb{R}_i being an internal node is the parent of \mathbb{R}_u and \mathbb{R}_u is a leaf node</li></ul>
         * <p>
         * or term 2:
         * <ul><li>|(\mathbb{R}_i \leftsemijoin J_{i+1}^*) \antijoin \tilde{\matbb{R}_u}|
         * where R_i is an internal relation and is the parent of R_u, which is a non-leaf internal relation</li></ul>
         * <p>
         * in the cost model. In the following, we take JOB Q1a as an example, the join tree is
         * <pre>
         * {@code
         * imdb.q1a_info_type(info_type_id)
         * |imdb_int.movie_info_idx(movie_id,info_type_id)
         * ||imdb.q1a_movie_companies(movie_id,company_id,company_type_id)
         * |||imdb.q1a_company_type(company_type_id)
         * |||imdb_int.title(movie_id,kind_id)
         * }
         * </pre>
         * <p>
         * <strong>Estimation to term 1</strong>
         * <p>
         * imdb.q1a_movie_companies is the parent of leaf imdb.q1a_company_type. Thus, by
         * the formula, we generate the following SQL query:
         * <p>
         * <pre>
         * {@code
         * EXPLAIN (FORMAT JSON) SELECT *
         * FROM imdb.q1a_movie_companies
         * WHERE movie_id in (SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx)
         *   and company_type_id not in (SELECT company_type_id from imdb.q1a_company_type)
         * }
         * </pre>
         * <p>
         * <strong>Estimation to term 2</strong>
         * <p>
         * imdb_int.movie_info_idx is the parent of non-leaf internal relation imdb.q1a_movie_companies. Thus, by
         * the formula, we generate the following SQL query:
         * <p>
         * <pre>
         * {@code
         * EXPLAIN (FORMAT JSON) SELECT *
         * FROM imdb_int.movie_info_idx
         * WHERE info_type_id in (SELECT info_type_id from imdb.q1a_info_type)
         * and movie_id not in (SELECT movie_id from imdb.q1a_movie_companies
         *                      EXCEPT ALL
         *                      SELECT movie_id from imdb.q1a_movie_companies
         *                      WHERE movie_id in (SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx)
         *                        and company_type_id not in (SELECT company_type_id from imdb.q1a_company_type)
         *                      EXCEPT ALL
         *                      SELECT movie_id from imdb.q1a_movie_companies
         *                      WHERE movie_id in (SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx)
         *                        and movie_id not in (SELECT movie_id from imdb_int.title));
         * }
         * </pre>
         * Note in the perfect exact equation, \mathbb{R}_i is replaced by \phyR_i^{[t]}. However, both examples above
         * are still correct.
         */
        private String buildCostSqlNonRoot(String catalog,
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
                    joinTree,
                    true,
                    false));
            return sql.toString();
        }

        /**
         * This function computes \tilde{\mathbb{R}_u}, the content of a relation after TTJ execution, i.e., in clean
         * state.
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
         * To compute content of q1a_movie_companies (targetNode) with movie_id as selected columns, the function
         * generates the following SQL:
         * <pre>
         * {@code
         * SELECT movie_id
         * FROM imdb.q1a_movie_companies
         * EXCEPT ALL
         * SELECT movie_id
         * FROM imdb.q1a_movie_companies
         * WHERE movie_id in (SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx)
         *   and company_type_id not in (SELECT company_type_id from imdb.q1a_company_type)
         * EXCEPT ALL
         * SELECT movie_id
         * FROM imdb.q1a_movie_companies
         * WHERE movie_id in (SELECT movie_id from imdb.q1a_info_type natural join imdb_int.movie_info_idx)
         *   and movie_id not in (SELECT movie_id from imdb_int.title));
         * }
         * </pre>
         */
        private String buildTildeMathBBRuSql(List<String> selectedColumns,
                                             String catalog,
                                             MultiwayJoinNode targetNode,
                                             MultiwayJoinOrderedGraph joinTree)
        {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ")
                    .append(Joiner.on(",").join(selectedColumns))
                    .append(" FROM ")
                    .append(getFullQualifiedTable(catalog, targetNode.getSchemaTableName()));
            if (targetNode.getNodeType() == NodeType.Leaf) {
                return sql.toString();
            }
            List<MultiwayJoinNode> children = joinTree.getChildren().get(targetNode);
            List<String> exceptSQLs = new ArrayList<>();
            for (MultiwayJoinNode child : children) {
                exceptSQLs.add(buildCostSqlNonRoot(catalog, false, selectedColumns, targetNode, child,
                        List.of(), joinTree));
            }
            sql.append(" EXCEPT ALL")
                    .append(Joiner.on(" EXCEPT ALL").join(exceptSQLs));
            return sql.toString();
        }

        private String buildTildeMathBBRuSqlOrNegate(List<String> selectedColumns,
                                                     String catalog,
                                                     MultiwayJoinNode targetNode,
                                                     List<MultiwayJoinNode> childNodes,
                                                     List<MultiwayJoinNode> prevVisitedChildren,
                                                     MultiwayJoinOrderedGraph joinTree,
                                                     boolean negate,
                                                     boolean addDistinct)
        {
            checkArgument(targetNode.getNodeType() != NodeType.None, targetNode + "has NodeType.None");
            if (Switches.DEBUG && traceLogger.isDebugEnabled()) {
                traceLogger.debug("targetNode: " + targetNode);
                traceLogger.debug("targetNode nodeType: " + targetNode.getNodeType());
                traceLogger.debug("childNodes: " + childNodes);
            }
            String inOrNotInOperator = negate ? "NOT IN" : "IN";
            String distinct = "";
            if (addDistinct) {
                checkState(targetNode.getNodeType() == NodeType.Root,
                        "distinct is only applicable to root node");
                checkState(childNodes.size() == 1, "childNodes.size() should be 1 when use distinct");
                distinct = "distinct";
            }
            StringBuilder builder = new StringBuilder();
            if (targetNode.getNodeType() == NodeType.Leaf) {
                builder.append("SELECT ")
                        .append(Joiner.on(",").join(selectedColumns))
                        .append(" FROM ")
                        .append(buildFromClause(selectedColumns, catalog, targetNode, prevVisitedChildren, joinTree));
                return builder.toString();
            }

            List<String> whereClauses = new ArrayList<>();
            Pair<List<String>, String> pair = getNaturalJoinQuery(catalog, targetNode, false, joinTree);
            if (pair != null) {
                whereClauses.add("(" + Joiner.on(",").join(pair.getKey()) + ")"
                        + " IN (" + pair.getValue() + ")");
            }

            for (MultiwayJoinNode child : childNodes) {
                List<String> sharedAttributes = sharedAttributes(targetNode, child);
                checkState(!sharedAttributes.isEmpty(),
                        "sharedAttributes from " + targetNode + " and " + child + " cannot be empty");
                whereClauses.add("(" + Joiner.on(",").join(sharedAttributes)
                        + ") "
                        + inOrNotInOperator
                        + " ("
                        + buildTildeMathBBRuSql(sharedAttributes, catalog, child, joinTree)
                        + ")");
            }

            builder.append(" SELECT ")
                    .append(distinct)
                    .append(addDistinct ? "(" : "");
            if (addDistinct) {
                List<String> sharedAttributes = new ArrayList<>(childNodes.get(0).getAttributes());
                sharedAttributes.retainAll(targetNode.getAttributes());
                builder.append(Joiner.on(",").join(sharedAttributes));
            }
            else {
                builder.append(Joiner.on(",").join(selectedColumns));
            }
            builder.append(addDistinct ? ")" : "")
                    .append(" FROM ")
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
         * Implements \phyR_i^{[t]} by using EXCEPT ALL.
         * <p>
         * As an example, consider the following join tree
         * <pre>
         * {@code
         * multiwaycomplex.TestCostModelCases_S(x,y,z)
         * |multiwaycomplex.TestCostModelCases_B(z)
         * |multiwaycomplex.TestCostModelCases_R(y,z)
         * }
         * </pre>
         * When generating SQL for S with respect to its child R, we have to take the impact of S due to B into consideration.
         * According cost-model6, the generated SQL is
         * <pre>
         * {@code
         * EXPLAIN (SETTINGS true, COSTS true, FORMAT JSON, ANALYZE)
         * SELECT distinct(x,y,z)
         * FROM (SELECT x,y,z
         *       FROM "postgres"."multiwaycomplex"."TestCostModelCases_S"
         *       EXCEPT ALL
         *       SELECT x,y,z
         *       FROM "postgres"."multiwaycomplex"."TestCostModelCases_S"
         *       WHERE (z) NOT IN (SELECT z
         *                         FROM "postgres"."multiwaycomplex"."TestCostModelCases_B")) AS alias0
         * WHERE (y,z) NOT IN (SELECT y,z FROM "postgres"."multiwaycomplex"."TestCostModelCases_R")
         * }
         * </pre>
         */
        private String buildFromClause(List<String> selectedColumns,
                                       String catalog,
                                       MultiwayJoinNode targetNode,
                                       List<MultiwayJoinNode> prevVisitedChildren,
                                       MultiwayJoinOrderedGraph joinTree)
        {
            if (targetNode.getNodeType() == NodeType.Leaf || prevVisitedChildren.isEmpty()) {
                return getFullQualifiedTable(catalog, targetNode.getSchemaTableName());
            }
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("(SELECT ")
                    .append(Joiner.on(",").join(selectedColumns))
                    .append(" FROM ")
                    .append(getFullQualifiedTable(catalog, targetNode.getSchemaTableName()));
            List<String> exceptSQLs = new ArrayList<>();
            for (MultiwayJoinNode prevChild : prevVisitedChildren) {
                exceptSQLs.add(buildCostSqlNonRoot(catalog, false, targetNode.getAttributes(), targetNode, prevChild,
                        List.of(), joinTree));
            }
            stringBuilder.append(" EXCEPT ALL")
                    .append(Joiner.on(" EXCEPT ALL").join(exceptSQLs))
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
                .numJoinTreeCosted(numJoinTreeCosted)
                .build();
    }
}
