package org.zhu45.treetracker.relational.operator;

public enum JoinOperator
{
    HASH_JOIN,
    TTJ,
    TTJHP,
    TTJHP_NO_NG,
    TTJHP_NO_DP,
    TTJHP_VANILLA,
    TTJHP_BF,
    TTJHP_BG,
    TTJHP_INT, // Purely for testing. For benchmarking, we run TupleBasedHighPerfTreeTrackerOneBetaHashTableIntOperator under TTJHP
    TTJV1,
    TTJV2,
    LIP,
    Yannakakis,
    YannakakisB,
    Yannakakis1Pass,
    PTO,
    YannakakisVanilla,
    BloomSemiJoin,
    SemiJoin
}
