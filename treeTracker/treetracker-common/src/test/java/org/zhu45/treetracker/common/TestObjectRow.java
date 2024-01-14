package org.zhu45.treetracker.common;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.row.ObjectRow;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.common.type.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

public class TestObjectRow
{
    CharType charType = createCharType(20);

    @Test
    public void testHashcode()
    {
        List<String> attributes = List.of("developer", "service");
        ObjectRow row1 = new ObjectRow(attributes, List.of(new StringValue(charType, "Zeyuan"), new StringValue(charType, "AWS Timestream")));
        ObjectRow row2 = new ObjectRow(attributes, List.of(new StringValue(charType, "Zeyuan"), new StringValue(charType, "AWS Timestream")));
        assertEquals(row1.hashCode(), row2.hashCode());
    }

    @Test
    public void testEqualsIgnoreOrder()
    {
        List<String> attributes = List.of("name", "age");
        ObjectRow row1 = new ObjectRow(attributes, List.of(new StringValue(charType, "Ava"), new StringValue(charType, "15")));
        ObjectRow row2 = new ObjectRow(attributes, List.of(new StringValue(charType, "15"), new StringValue(charType, "Ava")));
        assertTrue(ObjectRow.equalsIgnoreOrder(row1, row2));
    }

    @Test
    public void testGetEntryColumnHeaderHashCode()
    {
        List<String> attributes = List.of("developer", "service");
        ObjectRow row1 = new ObjectRow(attributes, List.of(new StringValue(charType, "Zeyuan"), new StringValue(charType, "AWS Timestream")));
        ObjectRow row2 = new ObjectRow(attributes, List.of(new StringValue(charType, "Yixuan"), new StringValue(charType, "Amazon Business")));
        for (int i = 0; i < row1.size(); ++i) {
            assertEquals(row1.getEntryColumnHeaderHashCode(i), row2.getEntryColumnHeaderHashCode(i));
        }
    }

    /**
     * Check the fulfillment of contract on hashcode() and equals() implementation:
     * According to <a href="https://docs.oracle.com/en/java/javase/12/docs/api/java.base/java/lang/Object.html#hashCode()">hashcode() java doc</a>,
     * "If two objects are equal according to the equals(Object) method, then calling the hashCode method on each of
     * the two objects must produce the same integer result."
     */
    @Test
    public void testEqualsAndHashCode()
    {
        List<String> attributes = Arrays.asList("name", "age");
        List<Type> types = new ArrayList<>(
                Arrays.asList(charType, charType)
        );
        ObjectRow row1 = new ObjectRow(attributes, types, List.of(new StringValue(charType, "Ava"), new StringValue(charType, "15")));
        ObjectRow row2 = new ObjectRow(attributes, types, List.of(new StringValue(charType, "Ava"), new StringValue(charType, "15")));
        assertEquals(row1, row2);
        assertEquals(row1.hashCode(), row2.hashCode());
    }
}
