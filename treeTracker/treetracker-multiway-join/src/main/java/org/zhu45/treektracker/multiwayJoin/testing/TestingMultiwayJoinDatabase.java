package org.zhu45.treektracker.multiwayJoin.testing;

import org.zhu45.treetracker.common.ColorValue;
import org.zhu45.treetracker.common.Column;
import org.zhu45.treetracker.common.Relation;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.JdbcTableHandle;
import org.zhu45.treetracker.jdbc.testing.TestingDatabase;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.zhu45.treetracker.common.StandardErrorCode.NOT_SUPPORTED;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

public class TestingMultiwayJoinDatabase
        extends TestingDatabase
{
    public static final CharType charType = createCharType(10);
    public static final String schemaName = "multiway";
    private static final int numRelations = 6;

    public enum RelationData
    {
        A("A"),
        B("B"),
        R("R"),
        S("S"),
        T("T"),
        U("U");

        private final String relationName;

        RelationData(String relationName)
        {
            this.relationName = relationName;
        }

        public String getRelationName()
        {
            return this.relationName;
        }

        public int getRelationSize()
        {
            switch (relationName) {
                case "A":
                    return 1;
                case "B":
                    return 2;
                case "R":
                    return 5;
                case "S":
                    return 7;
                case "T":
                    return 9;
                case "U":
                    return 9;
                default:
                    throw new TreeTrackerException(NOT_SUPPORTED, "there is no preset matching relationSize for the given relationName");
            }
        }

        private List<RelationalValue> createSingleColumnValues()
        {
            switch (relationName) {
                case "A":
                    return new ArrayList<>(singletonList(
                            new ColorValue(charType, ColorValue.Color.Red)));
                case "B":
                    return new ArrayList<>(Arrays.asList(
                            new ColorValue(charType, ColorValue.Color.Red),
                            new ColorValue(charType, ColorValue.Color.Red)));
                case "R":
                    return new ArrayList<>(Arrays.asList(
                            new ColorValue(charType, ColorValue.Color.Black),
                            new ColorValue(charType, ColorValue.Color.Grey),
                            new ColorValue(charType, ColorValue.Color.Red),
                            new ColorValue(charType, ColorValue.Color.Orange),
                            new ColorValue(charType, ColorValue.Color.Blue)));
                case "S":
                    return new ArrayList<>(Arrays.asList(
                            new ColorValue(charType, ColorValue.Color.Black),
                            new ColorValue(charType, ColorValue.Color.Grey),
                            new ColorValue(charType, ColorValue.Color.Red),
                            new ColorValue(charType, ColorValue.Color.Orange),
                            new ColorValue(charType, ColorValue.Color.Navy),
                            new ColorValue(charType, ColorValue.Color.White),
                            new ColorValue(charType, ColorValue.Color.Green),
                            new ColorValue(charType, ColorValue.Color.Purple)));
                case "T":
                    return new ArrayList<>(Arrays.asList(
                            new ColorValue(charType, ColorValue.Color.Black),
                            new ColorValue(charType, ColorValue.Color.Grey),
                            new ColorValue(charType, ColorValue.Color.Red),
                            new ColorValue(charType, ColorValue.Color.White),
                            new ColorValue(charType, ColorValue.Color.Turquoise),
                            new ColorValue(charType, ColorValue.Color.Yellow),
                            new ColorValue(charType, ColorValue.Color.Green),
                            new ColorValue(charType, ColorValue.Color.Purple),
                            new ColorValue(charType, ColorValue.Color.Moss)));
                case "U":
                    return new ArrayList<>(Arrays.asList(
                            new ColorValue(charType, ColorValue.Color.Black),
                            new ColorValue(charType, ColorValue.Color.Grey),
                            new ColorValue(charType, ColorValue.Color.White),
                            new ColorValue(charType, ColorValue.Color.White),
                            new ColorValue(charType, ColorValue.Color.Sky),
                            new ColorValue(charType, ColorValue.Color.Yellow),
                            new ColorValue(charType, ColorValue.Color.Green),
                            new ColorValue(charType, ColorValue.Color.White),
                            new ColorValue(charType, ColorValue.Color.Moss)));
                default:
                    throw new TreeTrackerException(NOT_SUPPORTED, "there is no preset matching column values for the given relationName");
            }
        }
    }

    public TestingMultiwayJoinDatabase()
    {
        super();
        try (Connection connection = getJdbcClient().getConnection()) {
            connection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        for (RelationData relation : RelationData.values()) {
            String relationName = relation.getRelationName();
            createSingleColumnRelation(relationName, relation.createSingleColumnValues());
        }
    }

    public TestingMultiwayJoinDatabase(JdbcClient jdbcClient)
    {
        super(jdbcClient);
        try (Connection connection = getJdbcClient().getConnection()) {
            connection.createStatement().execute("CREATE SCHEMA IF NOT EXISTS " + schemaName);
        }
        catch (SQLException e) {
            throw new RuntimeException(e);
        }
        for (RelationData relation : RelationData.values()) {
            String relationName = relation.getRelationName();
            createSingleColumnRelation(relationName, relation.createSingleColumnValues());
        }
    }

    @Override
    public String getSchemaName()
    {
        return schemaName;
    }

    private void createSingleColumnRelation(String relationName, List<RelationalValue> colors)
    {
        Column color = new Column("color", charType, colors);
        Relation relation = new Relation(relationName, singletonList(color));
        JdbcTableHandle tableHandle = getJdbcClient().getTableHandle(new SchemaTableName(schemaName, relationName));
        if (tableHandle != null) {
            getJdbcClient().deleteTable(tableHandle);
        }
        else {
            getJdbcClient().createTable(schemaName, relation);
        }
        getJdbcClient().insertTable(schemaName, relation);
    }

    @Override
    public List<SchemaTableName> getRelations()
    {
        List<SchemaTableName> relations = new ArrayList<>();
        for (TestingMultiwayJoinDatabase.RelationData relation : TestingMultiwayJoinDatabase.RelationData.values()) {
            relations.add(new SchemaTableName(schemaName, relation.getRelationName()));
        }
        return relations;
    }

    @Override
    public Optional<Long> getSeed()
    {
        return Optional.empty();
    }

    @Override
    public int getNumRelations()
    {
        return numRelations;
    }
}
