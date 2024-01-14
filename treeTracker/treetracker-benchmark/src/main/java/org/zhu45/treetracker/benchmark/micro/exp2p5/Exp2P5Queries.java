package org.zhu45.treetracker.benchmark.micro.exp2p5;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;

import java.util.HashMap;
import java.util.Map;

public enum Exp2P5Queries
        implements QueryEnum
{
    Exp2P5Query0P("Exp2P5Query0P"),
    Exp2P5Query10P("Exp2P5Query10P"),
    Exp2P5Query20P("Exp2P5Query20P"),
    Exp2P5Query30P("Exp2P5Query30P"),
    Exp2P5Query40P("Exp2P5Query40P"),
    Exp2P5Query50P("Exp2P5Query50P"),
    Exp2P5Query60P("Exp2P5Query60P"),
    Exp2P5Query70P("Exp2P5Query70P"),
    Exp2P5Query80P("Exp2P5Query80P"),
    Exp2P5Query90P("Exp2P5Query90P"),
    Exp2P5Query100P("Exp2P5Query100P");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (Exp2P5Queries c : values()) {
            switch (c) {
                case Exp2P5Query0P:
                    map.put(c.val, Exp2P5Query0P.class);
                    break;
                case Exp2P5Query10P:
                    map.put(c.val, Exp2P5Query10P.class);
                    break;
                case Exp2P5Query20P:
                    map.put(c.val, Exp2P5Query20P.class);
                    break;
                case Exp2P5Query30P:
                    map.put(c.val, Exp2P5Query30P.class);
                    break;
                case Exp2P5Query40P:
                    map.put(c.val, Exp2P5Query40P.class);
                    break;
                case Exp2P5Query50P:
                    map.put(c.val, Exp2P5Query50P.class);
                    break;
                case Exp2P5Query60P:
                    map.put(c.val, Exp2P5Query60P.class);
                    break;
                case Exp2P5Query70P:
                    map.put(c.val, Exp2P5Query70P.class);
                    break;
                case Exp2P5Query80P:
                    map.put(c.val, Exp2P5Query80P.class);
                    break;
                case Exp2P5Query90P:
                    map.put(c.val, Exp2P5Query90P.class);
                    break;
                case Exp2P5Query100P:
                    map.put(c.val, Exp2P5Query100P.class);
                    break;
            }
        }
    }

    private final String val;

    private Exp2P5Queries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
