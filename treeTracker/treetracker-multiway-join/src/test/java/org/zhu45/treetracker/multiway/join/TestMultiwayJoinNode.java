package org.zhu45.treetracker.multiway.join;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.RecordObjectTupleSourceProvider;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMultiwayJoinNode {
    private JdbcClient jdbcClient;
    private String schemaName;
    private ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        TestingMultiwayJoinDatabase database = new TestingMultiwayJoinDatabase();
        schemaName = database.getSchemaName();
        jdbcClient = database.getJdbcClient();
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testPopulateDomain() {
        TestingMultiwayJoinDatabase.RelationData relation = TestingMultiwayJoinDatabase.RelationData.R;
        String relationR = relation.getRelationName();
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);

        try (MultiwayJoinDomain domainR = new MultiwayJoinDomain(schemaTableNameR, jdbcClient, RecordObjectTupleSourceProvider.class)) {
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

            nodeR.populateDomain();
            assertEquals(nodeR.getDomain().size(), relation.getRelationSize());
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        connectionLeakUtil.assertNoLeaks();
    }
}
