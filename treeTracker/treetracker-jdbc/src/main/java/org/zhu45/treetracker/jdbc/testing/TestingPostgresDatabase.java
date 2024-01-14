package org.zhu45.treetracker.jdbc.testing;

import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.jdbc.JdbcClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;

public final class TestingPostgresDatabase
        extends TestingDatabase
{
    public static final String schemaName = "example";
    public static final String relationName = "numbers";
    public static final String intRelation = "numbers_with_int_type";

    public static final String col_text = "text";
    public static final String col_num = "num";

    public static final CharType charType = CharType.createCharType(10);

    public TestingPostgresDatabase()
    {
        super();
        JdbcClient jdbcClient = getJdbcClient();
        jdbcClient.createSchema(schemaName);
        createRelationName();
        createIntRelation();
    }

    private void createRelationName()
    {
        SchemaTableName schemaTableNameD = new SchemaTableName(schemaName, relationName);
        if (getJdbcClient().getTableHandle(schemaTableNameD) == null) {
            List<List<RelationalValue>> relationValD = List.of(
                    List.of(new StringValue(charType, "one"), new IntegerValue(1)),
                    List.of(new StringValue(charType, "two"), new IntegerValue(2)),
                    List.of(new StringValue(charType, "three"), new IntegerValue(3)),
                    List.of(new StringValue(charType, "ten"), new IntegerValue(10)),
                    List.of(new StringValue(charType, "eleven"), new IntegerValue(11)),
                    List.of(new StringValue("twelve"), new IntegerValue(12)));
            getJdbcClient().ingestRelation(
                    schemaName,
                    relationName,
                    new ArrayList<>(Arrays.asList(col_text, col_num)),
                    new ArrayList<>(Arrays.asList(charType, INTEGER)),
                    relationValD);
        }
    }

    private void createIntRelation()
    {
        SchemaTableName schemaTableNameD = new SchemaTableName(schemaName, intRelation);
        if (getJdbcClient().getTableHandle(schemaTableNameD) == null) {
            List<List<RelationalValue>> relationValD = List.of(
                    List.of(new IntegerValue(1)),
                    List.of(new IntegerValue(2)),
                    List.of(new IntegerValue(3)),
                    List.of(new IntegerValue(10)),
                    List.of(new IntegerValue(11)),
                    List.of(new IntegerValue(12)));
            getJdbcClient().ingestRelation(
                    schemaName,
                    intRelation,
                    new ArrayList<>(Arrays.asList(col_num)),
                    new ArrayList<>(Arrays.asList(INTEGER)),
                    relationValD);
        }
    }
}
