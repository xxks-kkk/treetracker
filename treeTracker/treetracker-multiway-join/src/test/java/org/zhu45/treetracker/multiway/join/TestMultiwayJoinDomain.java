package org.zhu45.treetracker.multiway.join;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
import org.zhu45.treetracker.common.*;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.RecordObjectTupleSourceProvider;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestMultiwayJoinDomain {
    private TestingMultiwayJoinDatabase database;
    private JdbcClient jdbcClient;
    private String schemaName;
    private ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        database = new TestingMultiwayJoinDatabase();
        schemaName = database.getSchemaName();
        jdbcClient = database.getJdbcClient();
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testGetDomainAsRowSet() {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            TestingMultiwayJoinDatabase.RelationData relation = TestingMultiwayJoinDatabase.RelationData.R;
            String relationR = relation.getRelationName();
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);

            MultiwayJoinDomain domainR = new MultiwayJoinDomain(schemaTableNameR, jdbcClient, RecordObjectTupleSourceProvider.class);
            domainToBeClosed.add(domainR);
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

            nodeR.populateDomain();
            MultiwayJoinDomain domain = (MultiwayJoinDomain) nodeR.getDomain();
            RowSet rowSet = domain.getDomainAsRowSet();
            assertEquals(rowSet.size(), relation.getRelationSize());
        }
        finally {
            domainToBeClosed.forEach(MultiwayJoinDomain::close);
        }
    }

    @Test
    public void testSetRowSetAsDomain() {
        List<MultiwayJoinDomain> domainToBeClosed = new ArrayList<>();
        try {
            CharType charType = createCharType(20);
            List<String> attributes = new ArrayList<>(
                    Arrays.asList("developer", "service")
            );
            List<Type> types = new ArrayList<>(
                    Arrays.asList(charType, charType)
            );
            RowSet rowSet = new RowSet();
            rowSet.add(new ObjectRow(attributes, types, new ArrayList<>(Arrays.asList(
                    new StringValue(charType, "Zeyuan"), new StringValue(charType, "AWS Timestream")))));
            rowSet.add(new ObjectRow(attributes, types, new ArrayList<>(Arrays.asList(
                    new StringValue(charType, "Yixuan"), new StringValue(charType, "Amazon Business")))));

            String relationR = TestingMultiwayJoinDatabase.RelationData.R.getRelationName();
            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);

            MultiwayJoinDomain domainR = new MultiwayJoinDomain(schemaTableNameR, jdbcClient, RecordObjectTupleSourceProvider.class);
            domainToBeClosed.add(domainR);
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);
            MultiwayJoinDomain domain = nodeR.getDomain();
            domain.iterator().next();
            domain.setRowSetAsDomain(rowSet);
            assertEquals(domain.size(), rowSet.size());
            assertEquals(domain.getDomainAsRowSet(), rowSet);
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
