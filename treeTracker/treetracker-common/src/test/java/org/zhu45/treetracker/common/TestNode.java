package org.zhu45.treetracker.common;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestNode {
    @Test
    public void testNodeHashCode() {
        BaseDomain domain = new BaseDomain(
                new ColorValue(ColorValue.Color.Blue),
                new ColorValue(ColorValue.Color.Red),
                new ColorValue(ColorValue.Color.Green));
        BaseNode wa = new BaseNode("WA", new BaseDomain(domain));
        BaseNode wa2 = new BaseNode("WA", new BaseDomain(domain));
        wa.setConnected(Collections.singletonList(wa2));
        wa2.setConnected(Collections.singletonList(wa));
        assertEquals(wa.hashCode(), wa2.hashCode());
    }

    @Test
    public void testNodeEquals() {
        BaseDomain domain = new BaseDomain(
                new ColorValue(ColorValue.Color.Blue),
                new ColorValue(ColorValue.Color.Red),
                new ColorValue(ColorValue.Color.Green));
        BaseNode wa = new BaseNode("WA", new BaseDomain(domain));
        BaseNode wa2 = new BaseNode("WA", new BaseDomain(domain));
        wa.setConnected(Collections.singletonList(wa2));
        wa2.setConnected(Collections.singletonList(wa));
        assertEquals(wa, wa2);
    }
}
