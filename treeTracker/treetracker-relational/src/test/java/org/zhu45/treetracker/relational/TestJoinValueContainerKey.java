package org.zhu45.treetracker.relational;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.StringValue;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class TestJoinValueContainerKey
{
    @Test
    public void testJoinValueContainerKeyHashCodeAndEqual() {
        JoinValueContainerKey jav1 = new JoinValueContainerKey(
                Arrays.asList(new StringValue("3"), new StringValue("10")));
        JoinValueContainerKey jav2 = new JoinValueContainerKey(
                Arrays.asList(new StringValue("2"), new StringValue("20")));
        JoinValueContainerKey jav3 = new JoinValueContainerKey(
                Arrays.asList(new StringValue("2"), new StringValue("20")));
        assertNotEquals(jav1, jav2);
        assertEquals(jav1.hashCode(), jav2.hashCode());
        assertEquals(jav2, jav3);
        assertEquals(jav2.hashCode(), jav3.hashCode());
    }
}
