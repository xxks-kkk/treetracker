package org.zhu45.treektracker.multiwayJoin.testing;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.testing.Database;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.common.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.testing.TestUtils.getCurrentMethod;

/**
 * Generate a random query graph
 * <p>
 * implementation is based on p.18 of
 * <p>
 * Swami, A. 1989. Optimization of Large Join Queries. Ph.D. Dissertation,
 * Stanford University, Department of Computer Science.
 * <p>
 * WARNING: the graph structure produced is not strictly query graph (i.e., two relations that are not
 * involved in a join predicate may be connected via an edge as well). If you're looking for query graph
 * that satisfies join tree property (i.e., acyclic conjunctive query), use createJoinTree() under
 * TestingMultiwayJoinDatabaseComplex class. This class is kept for legacy reason because
 * monolithic algorithm can work on query graph produced by this class.
 */
public class RandomQueryGraphGenerator
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(RandomQueryGraphGenerator.class);

    private final Database database;
    private final JdbcClient jdbcClient;
    private Optional<Long> seed;

    public RandomQueryGraphGenerator(Database database, Optional<Long> seed)
    {
        this.database = database;
        this.jdbcClient = database.getJdbcClient();
        this.seed = seed;
    }

    public int getMaximumNumberOfSupportedRelationsInGraph()
    {
        return database.getRelations().size();
    }

    public void updateSeed(Optional<Long> seed)
    {
        this.seed = seed;
    }

    public Optional<Long> getSeed()
    {
        return this.seed;
    }

    /**
     * Create a query graph of size n. The query graph itself is a tree. In other words,
     * the procedure essentially creates a spanning tree of size n.
     *
     * @param n number of nodes used
     * @return a pair of the MultiwayJoinGraph and one MultiwayJoinNode of the graph
     */
    public <T extends MultiwayJoinDomain> Pair<MultiwayJoinGraph, MultiwayJoinNode> createGraph(int n,
            Class<? extends MultiwayJoinDomain> multiwayJoinDomainClazz)
    {
        List<MultiwayJoinNode> s = new ArrayList<>();
        List<SchemaTableName> relations = database.getRelations();
        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeLists = new ArrayList<>();

        Random rand = new Random();
        if (seed != null && seed.isPresent()) {
            rand.setSeed(seed.get());
        }
        log.info(String.format("[%s] seed for generating random query graph: %s", getCurrentMethod(), this.seed));

        if (n > relations.size()) {
            throw new TreeTrackerException(INVALID_PROCEDURE_ARGUMENT,
                    String.format("n is larger than the number of relations available in the provided database %s",
                            database.getClass().getName()));
        }
        try {
            Constructor<? extends MultiwayJoinDomain> constructor =
                    multiwayJoinDomainClazz.getConstructor();
            SchemaTableName reln = relations.get(rand.nextInt(relations.size()));
            T domainReln = (T) constructor.newInstance();
            MultiwayJoinNode nodeReln = new MultiwayJoinNode(reln, domainReln);
            s.add(nodeReln);
            relations.remove(reln);
            MultiwayJoinNode root = nodeReln;

            for (int i = 1; i < n; ++i) {
                reln = relations.get(rand.nextInt(relations.size()));
                domainReln = (T) constructor.newInstance();
                nodeReln = new MultiwayJoinNode(reln, domainReln);
                MultiwayJoinNode nodejoinReln = s.get(rand.nextInt(s.size()));
                edgeLists.add(asEdge(nodeReln, nodejoinReln));
                log.debugGreen(String.format("add edge: %s - %s", nodeReln, nodejoinReln));
                s.add(nodeReln);
                relations.remove(reln);
            }
            return Pair.of(new MultiwayJoinGraph(edgeLists), root);
        }
        catch (Exception e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
    }
}
