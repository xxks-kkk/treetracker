package org.zhu45.treetracker.benchmark.micro.exp2p8;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.Database;
import org.zhu45.treetracker.common.SchemaTableName;

/**
 * Exp 2.8: Impact of Backjumping
 */
public class Exp2P8Database
{
    private Exp2P8Database()
    {
    }

    public static final String exp2P8DatabaseSchema = "exp2p8";

    public static String t0k1n10 = "t0k1n10";
    public static String t1k1n10 = "t1k1n10";
    public static String t2k1n10 = "t2k1n10";
    public static String t0k2n10 = "t0k2n10";
    public static String t1k2n10 = "t1k2n10";
    public static String t2k2n10 = "t2k2n10";
    public static String t3k2n10 = "t3k2n10";
    public static String t0k3n10 = "t0k3n10";
    public static String t1k3n10 = "t1k3n10";
    public static String t2k3n10 = "t2k3n10";
    public static String t3k3n10 = "t3k3n10";
    public static String t4k3n10 = "t4k3n10";
    public static String t0k4n10 = "t0k4n10";
    public static String t1k4n10 = "t1k4n10";
    public static String t2k4n10 = "t2k4n10";
    public static String t3k4n10 = "t3k4n10";
    public static String t4k4n10 = "t4k4n10";
    public static String t5k4n10 = "t5k4n10";
    public static String t0k5n10 = "t0k5n10";
    public static String t1k5n10 = "t1k5n10";
    public static String t2k5n10 = "t2k5n10";
    public static String t3k5n10 = "t3k5n10";
    public static String t4k5n10 = "t4k5n10";
    public static String t5k5n10 = "t5k5n10";
    public static String t6k5n10 = "t6k5n10";
    public static String t0k6n10 = "t0k6n10";
    public static String t1k6n10 = "t1k6n10";
    public static String t2k6n10 = "t2k6n10";
    public static String t3k6n10 = "t3k6n10";
    public static String t4k6n10 = "t4k6n10";
    public static String t5k6n10 = "t5k6n10";
    public static String t6k6n10 = "t6k6n10";
    public static String t7k6n10 = "t7k6n10";
    public static String t0k7n10 = "t0k7n10";
    public static String t1k7n10 = "t1k7n10";
    public static String t2k7n10 = "t2k7n10";
    public static String t3k7n10 = "t3k7n10";
    public static String t4k7n10 = "t4k7n10";
    public static String t5k7n10 = "t5k7n10";
    public static String t6k7n10 = "t6k7n10";
    public static String t7k7n10 = "t7k7n10";
    public static String t8k7n10 = "t8k7n10";
    public static String t0k8n10 = "t0k8n10";
    public static String t1k8n10 = "t1k8n10";
    public static String t2k8n10 = "t2k8n10";
    public static String t3k8n10 = "t3k8n10";
    public static String t4k8n10 = "t4k8n10";
    public static String t5k8n10 = "t5k8n10";
    public static String t6k8n10 = "t6k8n10";
    public static String t7k8n10 = "t7k8n10";
    public static String t8k8n10 = "t8k8n10";
    public static String t9k8n10 = "t9k8n10";

    public static MultiwayJoinNode getTableNode(String exp2P8BaseRelation)
    {
        return Database.getTableNode(new SchemaTableName(exp2P8DatabaseSchema, exp2P8BaseRelation));
    }
}
