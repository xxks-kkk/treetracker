package org.zhu45.treetracker.multiway.join;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy;
import org.zhu45.treektracker.multiwayJoin.testing.RandomQueryGraphGenerator;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.jdbc.testing.Database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.GITHUB;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRandomQueryGraphGenerator
{
    private RandomQueryGraphGenerator generator;
    private static LoggerProvider.TreeTrackerLogger log = getLogger(TestRandomQueryGraphGenerator.class);

    private ConnectionLeakUtil connectionLeakUtil;
    private Database database;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestRandomQueryGraphGenerator.class.getName(), Level.INFO);
            Configurator.setAllLevels(RandomQueryGraphGenerator.class.getName(), Level.INFO);
        }

        database = new TestingMultiwayJoinDatabase();
        generator = new RandomQueryGraphGenerator(database, Optional.empty());
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testCreateGraph()
    {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            Pair<MultiwayJoinGraph, MultiwayJoinNode> pair =
                    generator.createGraph(
                            generator.getMaximumNumberOfSupportedRelationsInGraph(),
                            MultiwayJoinDomain.class);
            MultiwayJoinGraph graph = pair.getKey();
            List<MultiwayJoinNode> baseNodes = new ArrayList<>(pair.getKey().getNodes());
            domainToBeClosed.addAll(baseNodes.stream()
                    .map(MultiwayJoinNode::getDomain)
                    .collect(Collectors.toList()));
            assertEquals(graph.getNodes().size(), generator.getMaximumNumberOfSupportedRelationsInGraph());
        }
        catch (Exception e) {
            log.debugGreen(Arrays.toString(e.getStackTrace()));
        }
        finally {
            domainToBeClosed.forEach(MultiwayJoinDomain::close);
            optionallyCheckLeaks(connectionLeakUtil, "testCreateGraph");
        }
    }

    /**
     * Test whether createGraph() will create the exact same graphs given the same seed.
     */
    @Test
    public void testCreateGraphWithSeed()
    {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            generator.updateSeed(Optional.of(23L));
            Pair<MultiwayJoinGraph, MultiwayJoinNode> pair =
                    generator.createGraph(
                            generator.getMaximumNumberOfSupportedRelationsInGraph(),
                            MultiwayJoinDomain.class);
            List<MultiwayJoinNode> nodes1 = new ArrayList<>(pair.getKey().getNodes());
            domainToBeClosed.addAll(nodes1.stream()
                    .map(MultiwayJoinNode::getDomain)
                    .collect(Collectors.toList()));
            Pair<MultiwayJoinGraph, MultiwayJoinNode> pair2 =
                    generator.createGraph(
                            generator.getMaximumNumberOfSupportedRelationsInGraph(),
                            MultiwayJoinDomain.class);
            List<MultiwayJoinNode> nodes2 = new ArrayList<>(pair2.getKey().getNodes());
            domainToBeClosed.addAll(nodes2.stream()
                    .map(MultiwayJoinNode::getDomain)
                    .collect(Collectors.toList()));
            assertEquals(pair.getKey(), pair2.getKey());
            assertEquals(pair.getValue(), pair2.getValue());
        }
        finally {
            domainToBeClosed.forEach(MultiwayJoinDomain::close);
            optionallyCheckLeaks(connectionLeakUtil, "testCreateGraphWithSeed");
        }
    }

    @Test
    public void testGraphShouldBeTree()
            throws SQLException
    {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            generator.updateSeed(Optional.empty());
            int numberOfNodesUsed = ThreadLocalRandom.current().nextInt(1, generator.getMaximumNumberOfSupportedRelationsInGraph());
            log.debug("numberOfNodesUsed: " + numberOfNodesUsed);
            Pair<MultiwayJoinGraph, MultiwayJoinNode> pair =
                    generator.createGraph(numberOfNodesUsed, MultiwayJoinDomain.class);
            List<MultiwayJoinNode> baseNodes = new ArrayList<>(pair.getKey().getNodes());
            domainToBeClosed.addAll(baseNodes.stream()
                    .map(MultiwayJoinNode::getDomain)
                    .collect(Collectors.toList()));
            MultiwayJoinPreorderTraversalStrategy strategy = new MultiwayJoinPreorderTraversalStrategy(pair.getValue());
            MultiwayJoinOrderedGraph orderedGraph = strategy.traversal();
            assertTrue(orderedGraph.isOgATree());
        }
        finally {
            domainToBeClosed.forEach(MultiwayJoinDomain::close);
            database.getJdbcClient().getConnection().close();
            optionallyCheckLeaks(connectionLeakUtil, "testGraphShouldBeTree");
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        database.getJdbcClient().getConnection().close();
        optionallyCheckLeaks(connectionLeakUtil, null);
    }

    private static void optionallyCheckLeaks(ConnectionLeakUtil connectionLeakUtil, String methodName) {
        try {
            connectionLeakUtil.assertNoLeaks();
        }
        catch (RuntimeException e) {
            log.warn("TestRandomQueryGraphGenerator has connection leak (due to " + methodName);
            if (!checkEnvVariableSet(GITHUB)) {
                throw e;
            }
        }
    }
}
