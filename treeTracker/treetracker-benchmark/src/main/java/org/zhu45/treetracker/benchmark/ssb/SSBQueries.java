package org.zhu45.treetracker.benchmark.ssb;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;

import java.util.HashMap;
import java.util.Map;

public enum SSBQueries
        implements QueryEnum
{
    Q1P1("Q1P1"),
    Q1P2("Q1P2"),
    Q1P3("Q1P3"),
    Q2P1("Q2P1"),
    Q2P2("Q2P2"),
    Q2P3("Q2P3"),
    Q3P1("Q3P1"),
    Q3P2("Q3P2"),
    Q3P3("Q3P3"),
    Q3P4("Q3P4"),
    Q4P1("Q4P1"),
    Q4P2("Q4P2"),
    Q4P3("Q4P3");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (SSBQueries c : values()) {
            switch (c) {
                case Q1P1:
                    map.put(c.val, Query1P1.class);
                    break;
                case Q1P2:
                    map.put(c.val, Query1P2.class);
                    break;
                case Q1P3:
                    map.put(c.val, Query1P3.class);
                    break;
                case Q2P1:
                    map.put(c.val, Query2P1.class);
                    break;
                case Q2P2:
                    map.put(c.val, Query2P2.class);
                    break;
                case Q2P3:
                    map.put(c.val, Query2P3.class);
                    break;
                case Q3P1:
                    map.put(c.val, Query3P1.class);
                    break;
                case Q3P2:
                    map.put(c.val, Query3P2.class);
                    break;
                case Q3P3:
                    map.put(c.val, Query3P3.class);
                    break;
                case Q3P4:
                    map.put(c.val, Query3P4.class);
                    break;
                case Q4P1:
                    map.put(c.val, Query4P1.class);
                    break;
                case Q4P2:
                    map.put(c.val, Query4P2.class);
                    break;
                case Q4P3:
                    map.put(c.val, Query4P3.class);
                    break;
            }
        }
    }

    private final String val;

    private SSBQueries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
