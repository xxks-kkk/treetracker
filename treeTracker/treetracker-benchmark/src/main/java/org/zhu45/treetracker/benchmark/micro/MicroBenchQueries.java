package org.zhu45.treetracker.benchmark.micro;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.tpch.Query12WOptJoinTreeOptOrdering;

import java.util.HashMap;
import java.util.Map;

public enum MicroBenchQueries
        implements QueryEnum
{
    TwoWayJoinQuery1("TwoWayJoinQuery1"),
    PaperExampleQuery1("PaperExampleQuery1"),
    Query12WOptJoinTreeOptOrdering("Query12WOptJoinTreeOptOrdering"),
    Query12WOptJoinTreeOptOrderingAltOrder("Query12WOptJoinTreeOptOrderingAltOrder");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (MicroBenchQueries c : values()) {
            switch (c) {
                case TwoWayJoinQuery1:
                    map.put(c.val, TwoWayJoinQuery1.class);
                    break;
                case PaperExampleQuery1:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.PaperExampleQuery1.class);
                    break;
                case Query12WOptJoinTreeOptOrdering:
                    map.put(c.val, org.zhu45.treetracker.benchmark.tpch.Query12WOptJoinTreeOptOrdering.class);
                    break;
                case Query12WOptJoinTreeOptOrderingAltOrder:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.Query12WOptJoinTreeOptOrderingAltOrder.class);
                    break;
            }
        }
    }

    private final String val;

    private MicroBenchQueries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
