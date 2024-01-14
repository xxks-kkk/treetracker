package org.zhu45.treetracker.common;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.row.RowSet;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

public class TestObjectRowSet
{
    @Test
    public void testRenderOutput() {
        CharType charType = createCharType(20);
        List<String> attributes = new ArrayList<>(
                Arrays.asList("developer", "service")
        );
        List<Type> types = new ArrayList<>(
                Arrays.asList(charType, charType)
        );
        RowSet rowSet = new RowSet();
        rowSet.add(new ObjectRow(attributes, types, List.of(new StringValue(charType, "Zeyuan"), new StringValue(charType, "AWS Timestream"))));
        rowSet.add(new ObjectRow(attributes, types, List.of(new StringValue(charType, "Yixuan"), new StringValue(charType, "Amazon Business"))));
        String expected =
                "developer           |service             |\n" +
                "-----------------------------------------+\n" +
                "Zeyuan              |AWS Timestream      |\n" +
                "-----------------------------------------+\n" +
                "Yixuan              |Amazon Business     |\n" +
                "-----------------------------------------+\n";
        assertEquals(rowSet.renderOutput(), expected);
    }
}
