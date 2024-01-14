package org.zhu45.treetracker.multiway.join;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
import org.zhu45.treetracker.common.*;
import org.zhu45.treetracker.jdbc.*;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;

import java.util.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.StandardErrorCode.INVALID_USAGE;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMultiwayJoinGraph
{
    private TestingMultiwayJoinDatabase database;
    private JdbcClient jdbcClient;
    private Map<String, JdbcColumnHandle> columnHandles;
    private ObjectRecordTupleSource recordTupleSource;
    private RecordTupleSourceProvider recordTupleSourceProvider;
    private String schemaName;
    private ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        database = new TestingMultiwayJoinDatabase();
        schemaName = database.getSchemaName();
        jdbcClient = database.getJdbcClient();
        recordTupleSourceProvider = new RecordObjectTupleSourceProvider(new JdbcRecordSetProvider(jdbcClient));
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    /**
     * The graph is
     * R - S - T
     */
    @Test
    public void multiwayJoinGraph1()
    {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            String relationR = TestingMultiwayJoinDatabase.RelationData.R.getRelationName();
            String relationS = TestingMultiwayJoinDatabase.RelationData.S.getRelationName();
            String relationT = TestingMultiwayJoinDatabase.RelationData.T.getRelationName();
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);

            MultiwayJoinDomain domainR = new MultiwayJoinDomain();
            domainToBeClosed.add(domainR);
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);
            MultiwayJoinDomain domainS = new MultiwayJoinDomain();
            domainToBeClosed.add(domainS);
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);
            MultiwayJoinDomain domainT = new MultiwayJoinDomain();
            domainToBeClosed.add(domainT);
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

            List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                    asQueryGraphEdge(nodeR, nodeS),
                    asQueryGraphEdge(nodeS, nodeT)
            ));

            MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
            Set<MultiwayJoinNode> nodesSet = g.getNodes();
            assertEquals(nodesSet.size(), 3);

            for (MultiwayJoinNode baseNode : nodesSet) {
                String connectedString = baseNode.connectedToString();
                String nodeName = baseNode.getNodeName();
                if (nodeName.equals(schemaTableNameR.toString())) {
                    assertEquals(baseNode.getConnected().size(), 1);
                    assertTrue(connectedString.contains(schemaTableNameS.toString()));
                }
                else if (nodeName.equals(schemaTableNameS.toString())) {
                    assertEquals(baseNode.getConnected().size(), 2);
                    assertTrue(connectedString.contains(schemaTableNameR.toString()));
                    assertTrue(connectedString.contains(schemaTableNameT.toString()));
                }
                else if (nodeName.equals(schemaTableNameT.toString())) {
                    assertEquals(baseNode.getConnected().size(), 1);
                    assertTrue(connectedString.contains(schemaTableNameS.toString()));
                }
                else {
                    throw new TreeTrackerException(INVALID_USAGE, "Unknown nodeName: " + nodeName);
                }
            }
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
