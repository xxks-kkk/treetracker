package org.zhu45.treetracker.benchmark.ssb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;

import java.util.Map;
import java.util.Optional;

import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.customerDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.dateDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.lineOrderDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.partDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.ssbSchemaNameDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.supplierDev;
import static org.zhu45.treetracker.common.TestConstants.GITHUB;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestQueryOne
{
    private ConnectionLeakUtil connectionLeakUtil;
    private static Logger log = LogManager.getLogger(TestQueryOne.class);

    @BeforeAll
    public void setUp()
    {
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    /**
     * Can be used to create star schema benchmark database for development purpose
     */
    @Test
    @Order(1)
    public void testStarSchemaBenchmarkDatabaseDev()
    {
        String propertyFileName = "ssb.dev.properties";

        Map<String, String> env = System.getenv();
        Assumptions.assumingThat(!env.containsKey(GITHUB.getStringVal()), () -> {
            log.info("assumption satisfied and test StarSchemaBenchmarkDatabaseDev()");
           StarSchemaBenchmarkDatabase database = new StarSchemaBenchmarkDatabase(
                    ssbSchemaNameDev, customerDev, lineOrderDev, partDev, supplierDev, dateDev, propertyFileName);
        });
    }

    @Test
    public void testQueryOne()
    {
        Map<String, String> env = System.getenv();
        Assumptions.assumingThat(!env.containsKey(GITHUB.getStringVal()), () -> {
            JoinFragmentContext context = JoinFragmentContext.builder()
                    .setIsDevMode(true)
                    .setJdbcClient(postgresJdbcClientSupplier.get())
                    .build();
            QueryOne queryOne = new QueryOne(context);
            try {
                queryOne.eval();
            }
            finally {
                queryOne.getOperators().forEach(operator -> {
                    if (operator.getMultiwayJoinNode() != null) {
                        MultiwayJoinNode node = operator.getMultiwayJoinNode();
                        node.getDomain().close();
                    }
                    operator.close();
                });
            }
        });
    }

    /**
     * Test out constructor of QueryOne that takes in operatorMap.
     */
    @Test
    public void testQueryOneConstructor()
    {
        Map<String, String> env = System.getenv();
        Assumptions.assumingThat(!env.containsKey(GITHUB.getStringVal()), () -> {
            JoinFragmentContext context = JoinFragmentContext.builder()
                    .setIsDevMode(true)
                    .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                            Optional.of(TupleBasedHashJoinOperator.class)))
                    .setJdbcClient(postgresJdbcClientSupplier.get())
                    .build();
            QueryOne queryOne = new QueryOne(context);
            try {
                queryOne.eval();
            }
            finally {
                queryOne.getOperators().forEach(operator -> {
                    if (operator.getMultiwayJoinNode() != null) {
                        MultiwayJoinNode node = operator.getMultiwayJoinNode();
                        node.getDomain().close();
                    }
                    operator.close();
                });
            }
        });
    }

    @AfterAll
    public void assertNoLeaks()
    {
        connectionLeakUtil.assertNoLeaks();
    }
}