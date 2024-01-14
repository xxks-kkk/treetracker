package org.zhu45.treetracker.jdbc;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.row.IntRow;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.type.Type;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.jdbc.testing.Database;
import org.zhu45.treetracker.jdbc.testing.TestingPostgresDatabase;

import java.sql.SQLException;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestingPostgresDatabase.charType;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestRecordTupleSourceProvider
{
    private ConnectionLeakUtil connectionLeakUtil;
    private Database database;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        database = new TestingPostgresDatabase();
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testRecordObjectTupleSourceProvider()
            throws SQLException
    {
        try {
            JdbcClient jdbcClient = database.getJdbcClient();
            JdbcTableHandle jdbcTableHandle = database.getJdbcClient()
                    .getTableHandle(new SchemaTableName(TestingPostgresDatabase.schemaName, TestingPostgresDatabase.relationName));
            RecordObjectTupleSourceProvider recordTupleSourceProvider = new RecordObjectTupleSourceProvider(new JdbcRecordSetProvider(jdbcClient));
            List<JdbcColumnHandle> jdbcColumnHandleList = database.getJdbcClient().getColumns(jdbcTableHandle);
            ObjectRecordTupleSource recordTupleSource = recordTupleSourceProvider.createTupleSource(jdbcTableHandle, jdbcColumnHandleList);

            List<String> attributes = ImmutableList.of(TestingPostgresDatabase.col_text,
                    TestingPostgresDatabase.col_num);
            List<Type> types = ImmutableList.of(charType, INTEGER);
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new ObjectRow(attributes, types, List.of(new StringValue(charType, "one"), new IntegerValue(1))));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new ObjectRow(attributes, types, List.of(new StringValue(charType, "two"), new IntegerValue(2))));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new ObjectRow(attributes, types, List.of(new StringValue(charType, "three"), new IntegerValue(3))));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new ObjectRow(attributes, types, List.of(new StringValue(charType, "ten"), new IntegerValue(10))));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new ObjectRow(attributes, types, List.of(new StringValue(charType, "eleven"), new IntegerValue(11))));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new ObjectRow(attributes, types, List.of(new StringValue(charType, "twelve"), new IntegerValue(12))));
            assertFalse(recordTupleSource.hasNext());
            assertThrows(NoSuchElementException.class, recordTupleSource::getNextRow);
        }
        finally {
            database.getJdbcClient().getConnection().close();
        }
    }

    @Test
    public void testRecordIntTupleSourceProvider()
            throws SQLException
    {
        try {
            JdbcClient jdbcClient = database.getJdbcClient();
            JdbcTableHandle jdbcTableHandle = database.getJdbcClient()
                    .getTableHandle(new SchemaTableName(TestingPostgresDatabase.schemaName, TestingPostgresDatabase.intRelation));
            RecordIntTupleSourceProvider recordTupleSourceProvider = new RecordIntTupleSourceProvider(new JdbcRecordSetProvider(jdbcClient));
            List<JdbcColumnHandle> jdbcColumnHandleList = database.getJdbcClient().getColumns(jdbcTableHandle);
            IntRecordTupleSource recordTupleSource = recordTupleSourceProvider.createTupleSource(jdbcTableHandle, jdbcColumnHandleList);

            List<String> attributes = ImmutableList.of(TestingPostgresDatabase.col_num);
            List<Type> types = ImmutableList.of(INTEGER);
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new IntRow(attributes, new int[]{1}));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new IntRow(attributes, new int[]{2}));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new IntRow(attributes, new int[]{3}));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new IntRow(attributes, new int[]{10}));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new IntRow(attributes, new int[]{11}));
            assertTrue(recordTupleSource.hasNext());
            assertEquals(recordTupleSource.getNextRow(),
                    new IntRow(attributes, new int[]{12}));
            assertFalse(recordTupleSource.hasNext());
            assertThrows(NoSuchElementException.class, recordTupleSource::getNextRow);
        }
        finally {
            database.getJdbcClient().getConnection().close();
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        connectionLeakUtil.assertNoLeaks();
    }
}
