package org.zhu45.treetracker.benchmark.tpch;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.common.SchemaTableName;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.zhu45.treetracker.benchmark.Database.getTableNode;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.createQueryName2Template;

public class TPCHDatabase
{
    private TPCHDatabase()
    {
    }

    public static final String schemaName = "tpch";
    public static final String schemaNameInt = "tpch_int";

    private static final String nation = "nation";
    private static final String region = "region";
    private static final String part = "part";
    private static final String supplier = "supplier";
    private static final String partsupp = "partsupp";
    private static final String customer = "customer";
    private static final String orders = "orders";
    private static final String lineitem = "lineitem";

    public static class CodeGen
    {
        public static final Map<String, String> relation2MultiwayJoinNode = Map.ofEntries(
                entry(nation, "nationNode"),
                entry(nation + "1", "nationNode1"),
                entry(nation + "2", "nationNode2"),
                entry(region, "regionNode"),
                entry(part, "partNode"),
                entry(supplier, "supplierNode"),
                entry(partsupp, "partSuppNode"),
                entry(customer, "customerNode"),
                entry(orders, "ordersNode"),
                entry(lineitem, "lineItemNode"));

        static List<String> queries = List.of("Query3W", "Query7aW", "Query7bW", "Query8W",
                "Query9W", "Query10W", "Query11W", "Query12W", "Query14W",
                "Query15W", "Query16W", "Query18W", "Query19aW", "Query19bW",
                "Query19cW", "Query20W");

        public static final String templatePathPrefix = Paths.get("codegen", "tpch", "templates").toString();

        public static final Map<String, String> queryName2Template = createQueryName2Template(queries, ".javat");

        public static final Map<String, String> queryName2TempateYannakakis = createQueryName2Template(queries, "OptJoinTreeOptOrderingY.javat");

        public static final Map<String, String> queryName2TempateYannakakisB = createQueryName2Template(queries, "OptJoinTreeOptOrderingYB.javat");

        public static final Map<String, String> queryName2TempateHJOnYannakakis = createQueryName2Template(queries, "OptJoinTreeOptOrderingHJOnY.javat");

        public static final String savePathPrefix = Paths.get("treetracker-benchmark",
                "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "tpch").toString();

        public static final Map<String, String> queryName2SavePath = createQueryName2SavePath();

        public static Map<String, String> createQueryName2SavePath()
        {
            Map<String, String> myMap = new HashMap<>();
            for (String query : queries) {
                myMap.put(query, "");
            }
            return myMap;
        }
    }

    public static MultiwayJoinNode getNation(TPCHQueries tpchQueries, TableInstanceId tableInstanceId)
    {
        switch (tpchQueries) {
            case Q11W:
            case Q7aW:
            case Q7bW:
            case Q8W:
            case Q20W:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, nation, tableInstanceId)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, nation));
        }
    }

    public static MultiwayJoinNode getRegion(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q8W:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, region, null)));
        }
        return getTableNode(new SchemaTableName(schemaName, region));
    }

    public static MultiwayJoinNode getPart(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q8W:
            case Q9W:
            case Q16W:
            case Q19aW:
            case Q19bW:
            case Q19cW:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, part, null)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, part));
        }
    }

    public static MultiwayJoinNode getSupplier(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q8W:
            case Q20W:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, supplier, null)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, supplier));
        }
    }

    public static MultiwayJoinNode getPartsupp(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q16W:
            case Q19aW:
            case Q19bW:
            case Q19cW:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, partsupp, null)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, partsupp));
        }
    }

    public static MultiwayJoinNode getCustomer(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q3W:
            case Q7aW:
            case Q7bW:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, customer, null)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, customer));
        }
    }

    public static MultiwayJoinNode getOrders(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q3W:
            case Q8W:
            case Q10W:
            case Q18W:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, orders, null)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, orders));
        }
    }

    public static MultiwayJoinNode getLineitem(TPCHQueries tpchQueries)
    {
        switch (tpchQueries) {
            case Q3W:
            case Q7aW:
            case Q7bW:
            case Q10W:
            case Q12W:
            case Q14W:
            case Q15W:
            case Q19aW:
            case Q19bW:
            case Q19cW:
                return getTableNode(new SchemaTableName(schemaName,
                        constructViewName(tpchQueries, lineitem, null)));
            default:
                return getTableNode(new SchemaTableName(schemaNameInt, lineitem));
        }
    }

    private static String constructViewName(TPCHQueries tpchQueries, String tableName, TableInstanceId tableInstanceId)
    {
        if (tableInstanceId != null) {
            return tpchQueries.name().toLowerCase() + "_" + tableName + tableInstanceId.getTableInstanceId();
        }
        else {
            return tpchQueries.name().toLowerCase() + "_" + tableName;
        }
    }
}
