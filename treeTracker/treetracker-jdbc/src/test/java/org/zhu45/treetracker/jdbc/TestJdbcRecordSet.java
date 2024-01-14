package org.zhu45.treetracker.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.jdbc.testing.TestingPostgresDatabase;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.zhu45.treetracker.common.type.CharType.CHAR;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.jdbc.TestingJdbcTypeHandle.JDBC_CHAR;
import static org.zhu45.treetracker.jdbc.TestingJdbcTypeHandle.JDBC_INTEGER;
import static org.zhu45.treetracker.jdbc.testing.TestingPostgresDatabase.charType;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestJdbcRecordSet {
    private JdbcClient jdbcClient;
    private JdbcTableHandle jdbcTableHandle;
    private List<JdbcColumnHandle> columnHandles;
    private ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp()
            throws Exception
    {
        TestingPostgresDatabase database = new TestingPostgresDatabase();
        jdbcClient = database.getJdbcClient();
        jdbcTableHandle = database.getJdbcClient()
                .getTableHandle(new SchemaTableName(TestingPostgresDatabase.schemaName, TestingPostgresDatabase.relationName));
        columnHandles = database.getJdbcClient().getColumns(jdbcTableHandle);
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testGetColumnTypes()
    {
        JdbcRecordSet recordSet = new JdbcRecordSet(jdbcClient, jdbcTableHandle, ImmutableList.of(
                new JdbcColumnHandle("test", TestingPostgresDatabase.col_text, JDBC_CHAR, CHAR, true),
                new JdbcColumnHandle("test", TestingPostgresDatabase.col_num, JDBC_INTEGER, INTEGER, true)));
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of(CHAR, INTEGER));

        recordSet = new JdbcRecordSet(jdbcClient, jdbcTableHandle, ImmutableList.of());
        assertEquals(recordSet.getColumnTypes(), ImmutableList.of());
    }

    @Test
    public void testCursorSimple()
    {
        RecordSet recordSet = new JdbcRecordSet(jdbcClient, jdbcTableHandle, columnHandles);

        try (RecordCursor cursor = recordSet.cursor()) {
            assertEquals(cursor.getType(0), charType);
            assertEquals(cursor.getType(1), INTEGER);

            Map<String, Long> data = new LinkedHashMap<>();
            while (cursor.advanceNextPosition()) {
                data.put(cursor.getString(0), cursor.getLong(1));
                assertFalse(cursor.isNull(0));
            }

            assertEquals(ImmutableMap.<String, Long>builder()
                    .put("one", 1L)
                    .put("two", 2L)
                    .put("three", 3L)
                    .put("ten", 10L)
                    .put("eleven", 11L)
                    .put("twelve", 12L)
                    .build(), data);
        }
    }

    @Test
    public void testIdempotentClose()
    {
        JdbcRecordSet recordSet = new JdbcRecordSet(jdbcClient, jdbcTableHandle, columnHandles);

        RecordCursor cursor = recordSet.cursor();
        cursor.close();
        cursor.close();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        connectionLeakUtil.assertNoLeaks();
    }
}
