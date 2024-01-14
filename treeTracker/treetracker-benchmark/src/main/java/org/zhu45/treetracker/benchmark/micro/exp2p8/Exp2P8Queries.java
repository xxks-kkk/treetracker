package org.zhu45.treetracker.benchmark.micro.exp2p8;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;

import java.util.HashMap;
import java.util.Map;

public enum Exp2P8Queries
        implements QueryEnum
{
    Exp2P8JoinTree1QueryK1N10("Exp2P8JoinTree1QueryK1N10"),
    Exp2P8JoinTree2QueryK1N10("Exp2P8JoinTree2QueryK1N10"),
    Exp2P8JoinTree1QueryK2N10("Exp2P8JoinTree1QueryK2N10"),
    Exp2P8JoinTree2QueryK2N10("Exp2P8JoinTree2QueryK2N10"),
    Exp2P8JoinTree1QueryK3N10("Exp2P8JoinTree1QueryK3N10"),
    Exp2P8JoinTree2QueryK3N10("Exp2P8JoinTree2QueryK3N10"),
    Exp2P8JoinTree1QueryK4N10("Exp2P8JoinTree1QueryK4N10"),
    Exp2P8JoinTree2QueryK4N10("Exp2P8JoinTree2QueryK4N10"),
    Exp2P8JoinTree1QueryK5N10("Exp2P8JoinTree1QueryK5N10"),
    Exp2P8JoinTree2QueryK5N10("Exp2P8JoinTree2QueryK5N10"),
    Exp2P8JoinTree1QueryK6N10("Exp2P8JoinTree1QueryK6N10"),
    Exp2P8JoinTree2QueryK6N10("Exp2P8JoinTree2QueryK6N10"),
    Exp2P8JoinTree1QueryK7N10("Exp2P8JoinTree1QueryK7N10"),
    Exp2P8JoinTree2QueryK7N10("Exp2P8JoinTree2QueryK7N10"),
    Exp2P8JoinTree1QueryK8N10("Exp2P8JoinTree1QueryK8N10"),
    Exp2P8JoinTree2QueryK8N10("Exp2P8JoinTree2QueryK8N10");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (Exp2P8Queries c : values()) {
            switch (c) {
                case Exp2P8JoinTree1QueryK1N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK1N10.class);
                    break;
                case Exp2P8JoinTree2QueryK1N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK1N10.class);
                    break;
                case Exp2P8JoinTree1QueryK2N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK2N10.class);
                    break;
                case Exp2P8JoinTree2QueryK2N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK2N10.class);
                    break;
                case Exp2P8JoinTree1QueryK3N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK3N10.class);
                    break;
                case Exp2P8JoinTree2QueryK3N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK3N10.class);
                    break;
                case Exp2P8JoinTree1QueryK4N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK4N10.class);
                    break;
                case Exp2P8JoinTree2QueryK4N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK4N10.class);
                    break;
                case Exp2P8JoinTree1QueryK5N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK5N10.class);
                    break;
                case Exp2P8JoinTree2QueryK5N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK5N10.class);
                    break;
                case Exp2P8JoinTree1QueryK6N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK6N10.class);
                    break;
                case Exp2P8JoinTree2QueryK6N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK6N10.class);
                    break;
                case Exp2P8JoinTree1QueryK7N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK7N10.class);
                    break;
                case Exp2P8JoinTree2QueryK7N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK7N10.class);
                    break;
                case Exp2P8JoinTree1QueryK8N10:
                    map.put(c.val, Exp2P8JoinTree1QueryK8N10.class);
                    break;
                case Exp2P8JoinTree2QueryK8N10:
                    map.put(c.val, Exp2P8JoinTree2QueryK8N10.class);
                    break;
            }
        }
    }

    private final String val;

    private Exp2P8Queries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
