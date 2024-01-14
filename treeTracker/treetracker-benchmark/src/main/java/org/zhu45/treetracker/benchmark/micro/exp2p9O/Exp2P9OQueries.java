package org.zhu45.treetracker.benchmark.micro.exp2p9O;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;

import java.util.HashMap;
import java.util.Map;

public enum Exp2P9OQueries
        implements QueryEnum
{
    Exp2P9Query0P("Exp2P9Query0P"),
    Exp2P9Query1P("Exp2P9Query1P"),
    Exp2P9Query2P("Exp2P9Query2P"),
    Exp2P9Query3P("Exp2P9Query3P"),
    Exp2P9Query4P("Exp2P9Query4P"),
    Exp2P9Query5P("Exp2P9Query5P"),
    Exp2P9Query6P("Exp2P9Query6P"),
    Exp2P9Query7P("Exp2P9Query7P"),
    Exp2P9Query8P("Exp2P9Query8P"),
    Exp2P9Query9P("Exp2P9Query9P"),
    Exp2P9Query10P("Exp2P9Query10P"),
    Exp2P9Query20P("Exp2P9Query20P"),
    Exp2P9Query30P("Exp2P9Query30P"),
    Exp2P9Query40P("Exp2P9Query40P"),
    Exp2P9Query50P("Exp2P9Query50P"),
    Exp2P9Query60P("Exp2P9Query60P"),
    Exp2P9Query70P("Exp2P9Query70P"),
    Exp2P9Query80P("Exp2P9Query80P"),
    Exp2P9Query90P("Exp2P9Query90P"),
    Exp2P9Query100P("Exp2P9Query100P");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (Exp2P9OQueries c : values()) {
            switch (c) {
                case Exp2P9Query0P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query0P.class);
                    break;
                case Exp2P9Query1P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query1P.class);
                    break;
                case Exp2P9Query2P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query2P.class);
                    break;
                case Exp2P9Query3P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query3P.class);
                    break;
                case Exp2P9Query4P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query4P.class);
                    break;
                case Exp2P9Query5P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query5P.class);
                    break;
                case Exp2P9Query6P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query6P.class);
                    break;
                case Exp2P9Query7P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query7P.class);
                    break;
                case Exp2P9Query8P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query8P.class);
                    break;
                case Exp2P9Query9P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query9P.class);
                    break;
                case Exp2P9Query10P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query10P.class);
                    break;
                case Exp2P9Query20P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query20P.class);
                    break;
                case Exp2P9Query30P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query30P.class);
                    break;
                case Exp2P9Query40P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query40P.class);
                    break;
                case Exp2P9Query50P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query50P.class);
                    break;
                case Exp2P9Query60P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query60P.class);
                    break;
                case Exp2P9Query70P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query70P.class);
                    break;
                case Exp2P9Query80P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query80P.class);
                    break;
                case Exp2P9Query90P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query90P.class);
                    break;
                case Exp2P9Query100P:
                    map.put(c.val, org.zhu45.treetracker.benchmark.micro.exp2p9O.Exp2P9Query100P.class);
                    break;
            }
        }
    }

    private final String val;

    private Exp2P9OQueries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
