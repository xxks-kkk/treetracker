package org.zhu45.treetracker.common;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestColorValue {
    @Test
    public void testHashCode() {
        ColorValue blue = new ColorValue(ColorValue.Color.Blue);
        ColorValue blue2 = new ColorValue(ColorValue.Color.Blue);
        assertEquals(blue.hashCode(), blue2.hashCode());
    }
}
