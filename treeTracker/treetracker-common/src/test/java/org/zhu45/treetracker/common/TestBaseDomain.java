package org.zhu45.treetracker.common;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;


public class TestBaseDomain {
    @Test
    public void testDomainIterator() {
        BaseDomain baseDomain = new BaseDomain(
                new ColorValue(ColorValue.Color.Blue),
                new ColorValue(ColorValue.Color.Red),
                new ColorValue(ColorValue.Color.Green));
        Iterator<Value> iter = baseDomain.iterator();
        BaseDomain testBaseDomain = new BaseDomain();
        while (iter.hasNext()) {
            testBaseDomain.add(iter.next());
        }
        assertEquals(testBaseDomain, baseDomain);
    }

    @Test
    public void testDomainCopyConstructor() {
        ColorValue blue = new ColorValue(ColorValue.Color.Blue);
        ColorValue red = new ColorValue(ColorValue.Color.Red);
        ColorValue green = new ColorValue(ColorValue.Color.Green);
        BaseDomain baseDomain = new BaseDomain(blue, red, green);
        BaseDomain copiedBaseDomain = new BaseDomain(baseDomain);
        baseDomain.remove(blue);
        assertEquals(baseDomain.size(), 2);
        assertEquals(copiedBaseDomain.size(), 3);
    }

    @Test
    public void testSetDomainVals() {
        ColorValue blue = new ColorValue(ColorValue.Color.Blue);
        ColorValue red = new ColorValue(ColorValue.Color.Red);
        ColorValue green = new ColorValue(ColorValue.Color.Green);
        BaseDomain baseDomain = new BaseDomain(blue, red, green);
        ColorValue yellow = new ColorValue(ColorValue.Color.Yellow);
        baseDomain.setDomainVals(Collections.singletonList(yellow));
        assertEquals(baseDomain.size(), 1);
        assertEquals(baseDomain.getDomainAsList(), Collections.singletonList(yellow));
    }

    @Test
    public void testHashCode() {
        ColorValue blue = new ColorValue(ColorValue.Color.Blue);
        ColorValue red = new ColorValue(ColorValue.Color.Red);
        ColorValue green = new ColorValue(ColorValue.Color.Green);
        BaseDomain baseDomain = new BaseDomain(blue, red, green);

        ColorValue blue2 = new ColorValue(ColorValue.Color.Blue);
        ColorValue red2 = new ColorValue(ColorValue.Color.Red);
        ColorValue green2 = new ColorValue(ColorValue.Color.Green);
        BaseDomain baseDomain2 = new BaseDomain(blue2, red2, green2);
        assertEquals(baseDomain.hashCode(), baseDomain2.hashCode());
    }

    @Test
    public void testEquals() {
        ColorValue blue = new ColorValue(ColorValue.Color.Blue);
        ColorValue red = new ColorValue(ColorValue.Color.Red);
        ColorValue green = new ColorValue(ColorValue.Color.Green);
        BaseDomain baseDomain = new BaseDomain(blue, red, green);

        ColorValue blue2 = new ColorValue(ColorValue.Color.Blue);
        ColorValue red2 = new ColorValue(ColorValue.Color.Red);
        ColorValue green2 = new ColorValue(ColorValue.Color.Green);
        BaseDomain baseDomain2 = new BaseDomain(blue2, red2, green2);
        assertEquals(baseDomain, baseDomain2);
    }
}
