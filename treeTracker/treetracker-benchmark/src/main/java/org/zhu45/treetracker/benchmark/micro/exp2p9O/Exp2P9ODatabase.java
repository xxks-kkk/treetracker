package org.zhu45.treetracker.benchmark.micro.exp2p9O;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.Database;
import org.zhu45.treetracker.common.SchemaTableName;

import java.util.Objects;

/**
 * Exp 2.9: Impact of Removing Dangling Tuples from R_{inner}
 */
public class Exp2P9ODatabase
{
    private Exp2P9ODatabase()
    {
    }

    public static final String exp2P9DatabaseSchema = "exp2p9";

    public static String exp2P9R = "r";
    public static String exp2P9U = "u";
    public static String exp2P9V = "v";
    public static String exp2P9T = "t";
    public static String exp2P9W = "w";

    public static MultiwayJoinNode getTableNode(String exp2P9BaseRelation, int semijoinmodratio)
    {
        if (Objects.equals(exp2P9BaseRelation, exp2P9U)) {
            return Database.getTableNode(new SchemaTableName(exp2P9DatabaseSchema, exp2P9BaseRelation + semijoinmodratio));
        }
        else {
            return Database.getTableNode(new SchemaTableName(exp2P9DatabaseSchema, exp2P9BaseRelation));
        }
    }
}
