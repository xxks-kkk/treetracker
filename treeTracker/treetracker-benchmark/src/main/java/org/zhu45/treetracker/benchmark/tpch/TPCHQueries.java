package org.zhu45.treetracker.benchmark.tpch;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;

import java.util.HashMap;
import java.util.Map;

public enum TPCHQueries
        implements QueryEnum
{
    Q2("Q2"),
    Q3("Q3"),
    Q3W("Q3W"),
    Q5("Q5"),
    Q7("Q7"),
    Q7aW("Q7aW"),
    Q7bW("Q7bW"),
    Q8("Q8"),
    Q8W("Q8W"),
    Q9("Q9"),
    Q9W("Q9W"),
    Q10("Q10"),
    Q10W("Q10W"),
    Q11("Q11"),
    Q11W("Q11W"),
    Q12("Q12"),
    Q12W("Q12W"),
    Q14("Q14"),
    Q14W("Q14W"),
    Q15("Q15"),
    Q15W("Q15W"),
    Q16("Q16"),
    Q16W("Q16W"),
    Q17("Q17"),
    Q18("Q18"),
    Q18W("Q18W"),
    Q19("Q19"),
    Q19aW("Q19aW"),
    Q19bW("Q19bW"),
    Q19cW("Q19cW"),
    Q20("Q20"),
    Q20W("Q20W"),
    Q21("Q21"),
    Q22("Q22");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (TPCHQueries c : values()) {
            switch (c) {
                case Q2:
                    map.put(c.val, Query2.class);
                    break;
                case Q3:
                    map.put(c.val, Query3.class);
                    break;
                case Q3W:
                    map.put(c.val, Query3W.class);
                    break;
                case Q7aW:
                    map.put(c.val, Query7aW.class);
                    break;
                case Q7bW:
                    map.put(c.val, Query7bW.class);
                    break;
                case Q8W:
                    map.put(c.val, Query8W.class);
                    break;
                case Q9:
                    map.put(c.val, Query9.class);
                    break;
                case Q9W:
                    map.put(c.val, Query9W.class);
                    break;
                case Q10:
                    map.put(c.val, Query10.class);
                    break;
                case Q10W:
                    map.put(c.val, Query10W.class);
                    break;
                case Q11:
                    map.put(c.val, Query11.class);
                    break;
                case Q11W:
                    map.put(c.val, Query11W.class);
                    break;
                case Q12:
                    map.put(c.val, Query12.class);
                    break;
                case Q12W:
                    map.put(c.val, Query12W.class);
                    break;
                case Q14:
                    map.put(c.val, Query14.class);
                    break;
                case Q14W:
                    map.put(c.val, Query14W.class);
                    break;
                case Q15:
                    map.put(c.val, Query15.class);
                    break;
                case Q15W:
                    map.put(c.val, Query15W.class);
                    break;
                case Q16:
                    map.put(c.val, Query16.class);
                    break;
                case Q16W:
                    map.put(c.val, Query16W.class);
                    break;
                case Q17:
                    map.put(c.val, Query17.class);
                    break;
                case Q18:
                    map.put(c.val, Query18.class);
                    break;
                case Q18W:
                    map.put(c.val, Query18W.class);
                    break;
                case Q19:
                    map.put(c.val, Query19.class);
                    break;
                case Q19aW:
                    map.put(c.val, Query19aW.class);
                    break;
                case Q19bW:
                    map.put(c.val, Query19bW.class);
                    break;
                case Q19cW:
                    map.put(c.val, Query19cW.class);
                    break;
                case Q20:
                    map.put(c.val, Query20.class);
                    break;
                case Q20W:
                    map.put(c.val, Query20W.class);
                    break;
                case Q21:
                    map.put(c.val, Query21.class);
                    break;
                case Q22:
                    map.put(c.val, Query22.class);
                    break;
            }
        }
    }

    private final String val;

    private TPCHQueries(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
