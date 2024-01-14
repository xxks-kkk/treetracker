package org.zhu45.treetracker.benchmark.micro.exp2p5;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.Database;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.Objects;

/**
 * Exp 2.5: Impact of No-good Tuples at R_k
 */
public class Exp2P5Database
{
    private Exp2P5Database()
    {
    }

    public static final String exp2P5DatabaseSchema = "exp2p5";

    public static String exp2P5T = "t";
    public static String exp2P5R = "r";
    public static String exp2P5S = "s";

    public static MultiwayJoinNode getTableNode(String exp2P5BaseRelation, int dupratio)
    {
        if (Objects.equals(exp2P5BaseRelation, exp2P5T)) {
            return Database.getTableNode(new SchemaTableName(exp2P5DatabaseSchema, exp2P5BaseRelation + dupratio));
        }
        else {
            return Database.getTableNode(new SchemaTableName(exp2P5DatabaseSchema, exp2P5BaseRelation));
        }
    }
}
