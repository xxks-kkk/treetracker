package org.zhu45.treetracker.benchmark.job;

import java.util.HashMap;
import java.util.Map;

public enum TableInstanceId
{
    ONE("ONE"),
    TWO("TWO");

    private static final Map<String, Integer> map = new HashMap<>(values().length, 1);

    static {
        for (TableInstanceId c : values()) {
            switch (c) {
                case ONE:
                    map.put(c.val, 1);
                    break;
                case TWO:
                    map.put(c.val, 2);
                    break;
            }
        }
    }

    private final String val;

    private TableInstanceId(String s)
    {
        val = s;
    }

    public Integer getTableInstanceId()
    {
        return map.get(val);
    }
}
