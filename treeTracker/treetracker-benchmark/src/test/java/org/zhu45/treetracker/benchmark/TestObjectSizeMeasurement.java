package org.zhu45.treetracker.benchmark;

import org.junit.jupiter.api.Test;

/**
 * This tests the measurement of size of data structure
 */
public class TestObjectSizeMeasurement
{
    @Test
    public void testJOL()
    {
        System.out.println("number of bytes: " + org.openjdk.jol.info.GraphLayout.parseInstance(4).totalSize());
        System.out.println("footprint: " + org.openjdk.jol.info.GraphLayout.parseInstance(4).toFootprint());
    }
}
