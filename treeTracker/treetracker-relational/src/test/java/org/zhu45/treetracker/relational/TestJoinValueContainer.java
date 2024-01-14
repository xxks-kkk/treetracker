package org.zhu45.treetracker.relational;

import org.junit.jupiter.api.Test;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.type.CharType;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

public class TestJoinValueContainer {
    private static CharType charType = createCharType(20);

    @Test
    public void testJoinValueContainerHashCodeAndEqual() {
        HashMap<JoinValueContainer, Integer> hashTableH = new HashMap<>();
        JoinValueContainer container = new JoinValueContainer();
        container.put("color", new StringValue(charType, "red"));
        hashTableH.put(container, 1992);
        assertEquals(hashTableH.get(container), 1992);
        JoinValueContainer container2 = new JoinValueContainer();
        container2.put("color", new StringValue(charType, "red"));
        assertEquals(hashTableH.get(container2), 1992);
        assertEquals(container, container2);
        assertEquals(container.hashCode(), container2.hashCode());
    }
}
