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
import org.zhu45.treektracker.multiwayJoin.testing.PostgresNaturalJoinExecutor;
import org.zhu45.treektracker.multiwayJoin.testing.RandomQueryGraphGenerator;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.logging.LoggerProvider;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.jdbc.testing.Database;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.logging.LoggerProvider.getLogger;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.naturalJoinJdbcClientSupplier;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPostgresNaturalJoinExecutor
{
    private static LoggerProvider.TreeTrackerLogger log = getLogger(TestPostgresNaturalJoinExecutor.class);

    private RandomQueryGraphGenerator generator;
    private Database database;
    private String naturalJoinTable = "naturalJoinTable";
    private ConnectionLeakUtil connectionLeakUtil;
    private PostgresNaturalJoinExecutor postgresNaturalJoinExecutor;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TestPostgresNaturalJoinExecutor.class.getName(), Level.INFO);
        }

        this.database = new TestingMultiwayJoinDatabase(naturalJoinJdbcClientSupplier.get());

        Random rand = new Random();
        long seed = rand.nextLong();
        generator = new RandomQueryGraphGenerator(database, Optional.of(seed));

        postgresNaturalJoinExecutor = new PostgresNaturalJoinExecutor(database, naturalJoinTable);
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testExecuteQueryGraphOnPostgres()
    {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            Pair<MultiwayJoinGraph, MultiwayJoinNode> pair =
                    generator.createGraph(
                            generator.getMaximumNumberOfSupportedRelationsInGraph(),
                            MultiwayJoinDomain.class);
            List<MultiwayJoinNode> baseNodes = new ArrayList<>(pair.getKey().getNodes());
            domainToBeClosed.addAll(baseNodes.stream()
                    .map(MultiwayJoinNode::getDomain)
                    .collect(Collectors.toList()));
            List<SchemaTableName> schemaTableNames = baseNodes.stream()
                    .map(MultiwayJoinNode::getSchemaTableName)
                    .collect(Collectors.toList());
            assertNotNull(postgresNaturalJoinExecutor.executeNaturalJoinOnPostgres(schemaTableNames));
        }
        finally {
            domainToBeClosed.forEach(MultiwayJoinDomain::close);
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        connectionLeakUtil.assertNoLeaks();
    }
}
