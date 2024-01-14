package org.zhu45.treetracker.benchmark.tupleFetch;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q8.Query8cOptJoinTreeOptOrdering;

import java.util.HashMap;
import java.util.Map;

public enum TupleFetchQueries
        implements QueryEnum
{
    TupleFetchQuery("TupleFetchQuery"),
    TupleFetchQuery2("TupleFetchQuery2"),
    Q8cOptJoinTreeOptOrdering("Q8cOptJoinTreeOptOrdering");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (TupleFetchQueries c : values()) {
            switch (c) {
                case TupleFetchQuery:
                    map.put(c.val, TupleFetchQuery.class);
                    break;
                case TupleFetchQuery2:
                    map.put(c.val, TupleFetchQuery2.class);
                    break;
                case Q8cOptJoinTreeOptOrdering:
                    map.put(c.val, Query8cOptJoinTreeOptOrdering.class);
                    break;
            }
        }
    }

    private final String val;

    private TupleFetchQueries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
