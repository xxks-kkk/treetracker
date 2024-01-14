package org.zhu45.treetracker.common;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.Utils.collectionCompare;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

public class TestColumn
{
    @BeforeAll
    static void setUp()
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(Column.class.getName(), Level.INFO);
        }
    }

    @Test
    public void testEqualsIgnoreOrder()
    {
        CharType charType = createCharType(10);
        List<String> attributes = new ArrayList<>(Arrays.asList(
                "age", "age2"
        ));
        List<Type> types = new ArrayList<>(
                Arrays.asList(charType, charType)
        );
        RowSet rowSet1 = new RowSet(List.of(
                new ObjectRow(attributes, List.of(new StringValue(charType, "1"), new StringValue(charType, "2"))),
                new ObjectRow(attributes, List.of(new StringValue(charType, "3"), new StringValue(charType, "4"))),
                new ObjectRow(attributes, List.of(new StringValue(charType, "5"), new StringValue(charType, "6"))),
                new ObjectRow(attributes, List.of(new StringValue(charType, "7"), new StringValue(charType,"4")))));
        RowSet rowSet2 = new RowSet(List.of(
                new ObjectRow(attributes,  List.of(new StringValue(charType, "2"), new StringValue(charType, "1"))),
                new ObjectRow(attributes, List.of(new StringValue(charType, "4"), new StringValue(charType, "5"))),
                new ObjectRow(attributes, List.of(new StringValue(charType, "4"), new StringValue(charType, "7"))),
                new ObjectRow(attributes, List.of(new StringValue(charType, "6"), new StringValue(charType,"3")))));
        Relation relation1 = new Relation(rowSet1);
        Relation relation2 = new Relation(rowSet2);
        assertTrue(collectionCompare(relation1.getColumns(), relation2.getColumns(), Column::equalsIgnoreOrder));
        assertFalse(collectionCompare(relation1.getRows(), relation2.getRows(), ObjectRow::equalsIgnoreOrder));
    }
}
