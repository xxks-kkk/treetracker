package org.zhu45.treetracker.benchmark.ssb;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Map.entry;
import static org.zhu45.treetracker.benchmark.Database.getTableNode;

public class SSBDatabase
{
    private SSBDatabase()
    {
    }

    private static final String ssbIntSchemaName = "ssb_int";
    private static final String ssbSchemaName = "ssb";

    public static final String customer = "customer";
    public static final String part = "part";
    public static final String supplier = "supplier";
    public static final String date = "date";
    public static final String lineorder = "lineorder";

    public static List<String> getTableNames()
    {
        return List.of(customer, part, supplier, date, lineorder);
    }

    public static class CodeGen
    {
        public static final Map<String, String> relation2MultiwayJoinNode = Map.ofEntries(
                entry(customer, "customerNode"),
                entry(part, "partNode"),
                entry(supplier, "supplierNode"),
                entry(date, "dateNode"),
                entry(lineorder, "lineOrderNode"));

        static List<String> queries = List.of("Query1P1", "Query1P2", "Query1P3", "Query2P1",
                "Query2P2", "Query2P3", "Query3P1", "Query3P2", "Query3P3",
                "Query3P4", "Query4P1", "Query4P2", "Query4P3");

        public static final String templatePathPrefix = Paths.get("codegen", "ssb", "templates").toString();

        public static final Map<String, String> queryName2Template = createQueryName2Template();

        public static Map<String, String> createQueryName2Template()
        {
            Map<String, String> myMap = new HashMap<>();
            for (String query : queries) {
                myMap.put(query, query + ".javat");
            }
            return myMap;
        }

        public static final String savePathPrefix = Paths.get("treetracker-benchmark",
                "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "ssb").toString();

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

    public static MultiwayJoinNode getCustomerInt(SSBQueries ssbQueries)
    {
        switch (ssbQueries) {
            case Q3P1:
            case Q3P2:
            case Q3P3:
            case Q3P4:
            case Q4P1:
            case Q4P2:
            case Q4P3:
                return getTableNode(new SchemaTableName(ssbSchemaName, constructViewName(ssbQueries, customer)));
            default:
                return getTableNode(new SchemaTableName(ssbIntSchemaName, customer));
        }
    }

    public static MultiwayJoinNode getPartInt(SSBQueries ssbQueries)
    {
        switch (ssbQueries) {
            case Q2P1:
            case Q2P2:
            case Q2P3:
            case Q4P1:
            case Q4P2:
            case Q4P3:
                return getTableNode(new SchemaTableName(ssbSchemaName, constructViewName(ssbQueries, part)));
            default:
                return getTableNode(new SchemaTableName(ssbIntSchemaName, part));
        }
    }

    public static MultiwayJoinNode getSupplierInt(SSBQueries ssbQueries)
    {
        switch (ssbQueries) {
            case Q2P1:
            case Q2P2:
            case Q2P3:
            case Q3P1:
            case Q3P2:
            case Q3P3:
            case Q3P4:
            case Q4P1:
            case Q4P2:
            case Q4P3:
                return getTableNode(new SchemaTableName(ssbSchemaName, constructViewName(ssbQueries, supplier)));
            default:
                return getTableNode(new SchemaTableName(ssbIntSchemaName, supplier));
        }
    }

    public static MultiwayJoinNode getDateInt(SSBQueries ssbQueries)
    {
        switch (ssbQueries) {
            case Q1P1:
            case Q1P2:
            case Q1P3:
            case Q3P1:
            case Q3P2:
            case Q3P3:
            case Q3P4:
            case Q4P2:
            case Q4P3:
                return getTableNode(new SchemaTableName(ssbSchemaName, constructViewName(ssbQueries, date)));
            default:
                return getTableNode(new SchemaTableName(ssbIntSchemaName, date));
        }
    }

    public static MultiwayJoinNode getLineOrderInt(SSBQueries ssbQueries)
    {
        switch (ssbQueries) {
            case Q1P1:
            case Q1P2:
            case Q1P3:
                return getTableNode(new SchemaTableName(ssbSchemaName, constructViewName(ssbQueries, lineorder)));
            default:
                return getTableNode(new SchemaTableName(ssbIntSchemaName, lineorder));
        }
    }

    private static String constructViewName(SSBQueries ssbQueries, String tableName)
    {
        return ssbQueries.name().toLowerCase() + "_" + tableName;
    }
}
