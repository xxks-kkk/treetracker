package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q1.Query1aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q1.Query1dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q10.Query10aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q10.Query10bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q10.Query10cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q11.Query11dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q12.Query12aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q12.Query12bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q12.Query12cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q13.Query13aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q13.Query13bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q13.Query13cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q13.Query13dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q14.Query14aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q14.Query14bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q14.Query14cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q15.Query15aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q15.Query15bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q15.Query15cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q15.Query15dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q16.Query16aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q16.Query16bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q16.Query16cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q16.Query16dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q17.Query17aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q17.Query17bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q17.Query17cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q17.Query17dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q17.Query17eFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q17.Query17fFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q18.Query18aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q18.Query18bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q18.Query18cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q19.Query19aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q19.Query19bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q19.Query19cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q19.Query19dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q2.Query2dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q20.Query20aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q20.Query20bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q20.Query20cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q21.Query21aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q21.Query21bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q21.Query21cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q22.Query22aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q22.Query22bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q22.Query22cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q22.Query22dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q23.Query23aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q23.Query23bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q23.Query23cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q24.Query24aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q24.Query24bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q25.Query25aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q25.Query25bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q25.Query25cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q26.Query26aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q26.Query26bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q26.Query26cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q27.Query27aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q27.Query27bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q27.Query27cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q28.Query28aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q28.Query28bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q28.Query28cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q29.Query29aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q29.Query29bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q29.Query29cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q3.Query3aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q3.Query3bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q3.Query3cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q30.Query30aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q30.Query30bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q30.Query30cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q31.Query31aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q31.Query31bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q31.Query31cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q32.Query32aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q32.Query32bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q33.Query33aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q33.Query33bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q33.Query33cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q4.Query4aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q4.Query4bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q4.Query4cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q5.Query5aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q5.Query5bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q5.Query5cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6eFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q6.Query6fFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q7.Query7aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q7.Query7bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q7.Query7cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q8.Query8dFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9aFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9bFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9cFindOptJoinTree;
import org.zhu45.treetracker.benchmark.job.q9.Query9dFindOptJoinTree;

import java.util.HashMap;
import java.util.Map;

/**
 * Generated by GenerateFindOptJoinTree
 */
public enum JOBQueriesFindOptJoinTree
        implements QueryEnum
{
    Query10aFindOptJoinTree("Query10aFindOptJoinTree"),
    Query10bFindOptJoinTree("Query10bFindOptJoinTree"),
    Query10cFindOptJoinTree("Query10cFindOptJoinTree"),
    Query11aFindOptJoinTree("Query11aFindOptJoinTree"),
    Query11bFindOptJoinTree("Query11bFindOptJoinTree"),
    Query11cFindOptJoinTree("Query11cFindOptJoinTree"),
    Query11dFindOptJoinTree("Query11dFindOptJoinTree"),
    Query12aFindOptJoinTree("Query12aFindOptJoinTree"),
    Query12bFindOptJoinTree("Query12bFindOptJoinTree"),
    Query12cFindOptJoinTree("Query12cFindOptJoinTree"),
    Query13aFindOptJoinTree("Query13aFindOptJoinTree"),
    Query13bFindOptJoinTree("Query13bFindOptJoinTree"),
    Query13cFindOptJoinTree("Query13cFindOptJoinTree"),
    Query13dFindOptJoinTree("Query13dFindOptJoinTree"),
    Query14aFindOptJoinTree("Query14aFindOptJoinTree"),
    Query14bFindOptJoinTree("Query14bFindOptJoinTree"),
    Query14cFindOptJoinTree("Query14cFindOptJoinTree"),
    Query15aFindOptJoinTree("Query15aFindOptJoinTree"),
    Query15bFindOptJoinTree("Query15bFindOptJoinTree"),
    Query15cFindOptJoinTree("Query15cFindOptJoinTree"),
    Query15dFindOptJoinTree("Query15dFindOptJoinTree"),
    Query16aFindOptJoinTree("Query16aFindOptJoinTree"),
    Query16bFindOptJoinTree("Query16bFindOptJoinTree"),
    Query16cFindOptJoinTree("Query16cFindOptJoinTree"),
    Query16dFindOptJoinTree("Query16dFindOptJoinTree"),
    Query17aFindOptJoinTree("Query17aFindOptJoinTree"),
    Query17bFindOptJoinTree("Query17bFindOptJoinTree"),
    Query17cFindOptJoinTree("Query17cFindOptJoinTree"),
    Query17dFindOptJoinTree("Query17dFindOptJoinTree"),
    Query17eFindOptJoinTree("Query17eFindOptJoinTree"),
    Query17fFindOptJoinTree("Query17fFindOptJoinTree"),
    Query18aFindOptJoinTree("Query18aFindOptJoinTree"),
    Query18bFindOptJoinTree("Query18bFindOptJoinTree"),
    Query18cFindOptJoinTree("Query18cFindOptJoinTree"),
    Query19aFindOptJoinTree("Query19aFindOptJoinTree"),
    Query19bFindOptJoinTree("Query19bFindOptJoinTree"),
    Query19cFindOptJoinTree("Query19cFindOptJoinTree"),
    Query19dFindOptJoinTree("Query19dFindOptJoinTree"),
    Query1aFindOptJoinTree("Query1aFindOptJoinTree"),
    Query1bFindOptJoinTree("Query1bFindOptJoinTree"),
    Query1cFindOptJoinTree("Query1cFindOptJoinTree"),
    Query1dFindOptJoinTree("Query1dFindOptJoinTree"),
    Query20aFindOptJoinTree("Query20aFindOptJoinTree"),
    Query20bFindOptJoinTree("Query20bFindOptJoinTree"),
    Query20cFindOptJoinTree("Query20cFindOptJoinTree"),
    Query21aFindOptJoinTree("Query21aFindOptJoinTree"),
    Query21bFindOptJoinTree("Query21bFindOptJoinTree"),
    Query21cFindOptJoinTree("Query21cFindOptJoinTree"),
    Query22aFindOptJoinTree("Query22aFindOptJoinTree"),
    Query22bFindOptJoinTree("Query22bFindOptJoinTree"),
    Query22cFindOptJoinTree("Query22cFindOptJoinTree"),
    Query22dFindOptJoinTree("Query22dFindOptJoinTree"),
    Query23aFindOptJoinTree("Query23aFindOptJoinTree"),
    Query23bFindOptJoinTree("Query23bFindOptJoinTree"),
    Query23cFindOptJoinTree("Query23cFindOptJoinTree"),
    Query24aFindOptJoinTree("Query24aFindOptJoinTree"),
    Query24bFindOptJoinTree("Query24bFindOptJoinTree"),
    Query25aFindOptJoinTree("Query25aFindOptJoinTree"),
    Query25bFindOptJoinTree("Query25bFindOptJoinTree"),
    Query25cFindOptJoinTree("Query25cFindOptJoinTree"),
    Query26aFindOptJoinTree("Query26aFindOptJoinTree"),
    Query26bFindOptJoinTree("Query26bFindOptJoinTree"),
    Query26cFindOptJoinTree("Query26cFindOptJoinTree"),
    Query27aFindOptJoinTree("Query27aFindOptJoinTree"),
    Query27bFindOptJoinTree("Query27bFindOptJoinTree"),
    Query27cFindOptJoinTree("Query27cFindOptJoinTree"),
    Query28aFindOptJoinTree("Query28aFindOptJoinTree"),
    Query28bFindOptJoinTree("Query28bFindOptJoinTree"),
    Query28cFindOptJoinTree("Query28cFindOptJoinTree"),
    Query29aFindOptJoinTree("Query29aFindOptJoinTree"),
    Query29bFindOptJoinTree("Query29bFindOptJoinTree"),
    Query29cFindOptJoinTree("Query29cFindOptJoinTree"),
    Query2aFindOptJoinTree("Query2aFindOptJoinTree"),
    Query2bFindOptJoinTree("Query2bFindOptJoinTree"),
    Query2cFindOptJoinTree("Query2cFindOptJoinTree"),
    Query2dFindOptJoinTree("Query2dFindOptJoinTree"),
    Query30aFindOptJoinTree("Query30aFindOptJoinTree"),
    Query30bFindOptJoinTree("Query30bFindOptJoinTree"),
    Query30cFindOptJoinTree("Query30cFindOptJoinTree"),
    Query31aFindOptJoinTree("Query31aFindOptJoinTree"),
    Query31bFindOptJoinTree("Query31bFindOptJoinTree"),
    Query31cFindOptJoinTree("Query31cFindOptJoinTree"),
    Query32aFindOptJoinTree("Query32aFindOptJoinTree"),
    Query32bFindOptJoinTree("Query32bFindOptJoinTree"),
    Query33aFindOptJoinTree("Query33aFindOptJoinTree"),
    Query33bFindOptJoinTree("Query33bFindOptJoinTree"),
    Query33cFindOptJoinTree("Query33cFindOptJoinTree"),
    Query3aFindOptJoinTree("Query3aFindOptJoinTree"),
    Query3bFindOptJoinTree("Query3bFindOptJoinTree"),
    Query3cFindOptJoinTree("Query3cFindOptJoinTree"),
    Query4aFindOptJoinTree("Query4aFindOptJoinTree"),
    Query4bFindOptJoinTree("Query4bFindOptJoinTree"),
    Query4cFindOptJoinTree("Query4cFindOptJoinTree"),
    Query5aFindOptJoinTree("Query5aFindOptJoinTree"),
    Query5bFindOptJoinTree("Query5bFindOptJoinTree"),
    Query5cFindOptJoinTree("Query5cFindOptJoinTree"),
    Query6aFindOptJoinTree("Query6aFindOptJoinTree"),
    Query6bFindOptJoinTree("Query6bFindOptJoinTree"),
    Query6cFindOptJoinTree("Query6cFindOptJoinTree"),
    Query6dFindOptJoinTree("Query6dFindOptJoinTree"),
    Query6eFindOptJoinTree("Query6eFindOptJoinTree"),
    Query6fFindOptJoinTree("Query6fFindOptJoinTree"),
    Query7aFindOptJoinTree("Query7aFindOptJoinTree"),
    Query7bFindOptJoinTree("Query7bFindOptJoinTree"),
    Query7cFindOptJoinTree("Query7cFindOptJoinTree"),
    Query8aFindOptJoinTree("Query8aFindOptJoinTree"),
    Query8bFindOptJoinTree("Query8bFindOptJoinTree"),
    Query8cFindOptJoinTree("Query8cFindOptJoinTree"),
    Query8dFindOptJoinTree("Query8dFindOptJoinTree"),
    Query9aFindOptJoinTree("Query9aFindOptJoinTree"),
    Query9bFindOptJoinTree("Query9bFindOptJoinTree"),
    Query9cFindOptJoinTree("Query9cFindOptJoinTree"),
    Query9dFindOptJoinTree("Query9dFindOptJoinTree");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (JOBQueriesFindOptJoinTree c : values()) {
            switch (c) {
                case Query10aFindOptJoinTree:
                    map.put(c.val, Query10aFindOptJoinTree.class);
                    break;
                case Query10bFindOptJoinTree:
                    map.put(c.val, Query10bFindOptJoinTree.class);
                    break;
                case Query10cFindOptJoinTree:
                    map.put(c.val, Query10cFindOptJoinTree.class);
                    break;
                case Query11aFindOptJoinTree:
                    map.put(c.val, Query11aFindOptJoinTree.class);
                    break;
                case Query11bFindOptJoinTree:
                    map.put(c.val, Query11bFindOptJoinTree.class);
                    break;
                case Query11cFindOptJoinTree:
                    map.put(c.val, Query11cFindOptJoinTree.class);
                    break;
                case Query11dFindOptJoinTree:
                    map.put(c.val, Query11dFindOptJoinTree.class);
                    break;
                case Query12aFindOptJoinTree:
                    map.put(c.val, Query12aFindOptJoinTree.class);
                    break;
                case Query12bFindOptJoinTree:
                    map.put(c.val, Query12bFindOptJoinTree.class);
                    break;
                case Query12cFindOptJoinTree:
                    map.put(c.val, Query12cFindOptJoinTree.class);
                    break;
                case Query13aFindOptJoinTree:
                    map.put(c.val, Query13aFindOptJoinTree.class);
                    break;
                case Query13bFindOptJoinTree:
                    map.put(c.val, Query13bFindOptJoinTree.class);
                    break;
                case Query13cFindOptJoinTree:
                    map.put(c.val, Query13cFindOptJoinTree.class);
                    break;
                case Query13dFindOptJoinTree:
                    map.put(c.val, Query13dFindOptJoinTree.class);
                    break;
                case Query14aFindOptJoinTree:
                    map.put(c.val, Query14aFindOptJoinTree.class);
                    break;
                case Query14bFindOptJoinTree:
                    map.put(c.val, Query14bFindOptJoinTree.class);
                    break;
                case Query14cFindOptJoinTree:
                    map.put(c.val, Query14cFindOptJoinTree.class);
                    break;
                case Query15aFindOptJoinTree:
                    map.put(c.val, Query15aFindOptJoinTree.class);
                    break;
                case Query15bFindOptJoinTree:
                    map.put(c.val, Query15bFindOptJoinTree.class);
                    break;
                case Query15cFindOptJoinTree:
                    map.put(c.val, Query15cFindOptJoinTree.class);
                    break;
                case Query15dFindOptJoinTree:
                    map.put(c.val, Query15dFindOptJoinTree.class);
                    break;
                case Query16aFindOptJoinTree:
                    map.put(c.val, Query16aFindOptJoinTree.class);
                    break;
                case Query16bFindOptJoinTree:
                    map.put(c.val, Query16bFindOptJoinTree.class);
                    break;
                case Query16cFindOptJoinTree:
                    map.put(c.val, Query16cFindOptJoinTree.class);
                    break;
                case Query16dFindOptJoinTree:
                    map.put(c.val, Query16dFindOptJoinTree.class);
                    break;
                case Query17aFindOptJoinTree:
                    map.put(c.val, Query17aFindOptJoinTree.class);
                    break;
                case Query17bFindOptJoinTree:
                    map.put(c.val, Query17bFindOptJoinTree.class);
                    break;
                case Query17cFindOptJoinTree:
                    map.put(c.val, Query17cFindOptJoinTree.class);
                    break;
                case Query17dFindOptJoinTree:
                    map.put(c.val, Query17dFindOptJoinTree.class);
                    break;
                case Query17eFindOptJoinTree:
                    map.put(c.val, Query17eFindOptJoinTree.class);
                    break;
                case Query17fFindOptJoinTree:
                    map.put(c.val, Query17fFindOptJoinTree.class);
                    break;
                case Query18aFindOptJoinTree:
                    map.put(c.val, Query18aFindOptJoinTree.class);
                    break;
                case Query18bFindOptJoinTree:
                    map.put(c.val, Query18bFindOptJoinTree.class);
                    break;
                case Query18cFindOptJoinTree:
                    map.put(c.val, Query18cFindOptJoinTree.class);
                    break;
                case Query19aFindOptJoinTree:
                    map.put(c.val, Query19aFindOptJoinTree.class);
                    break;
                case Query19bFindOptJoinTree:
                    map.put(c.val, Query19bFindOptJoinTree.class);
                    break;
                case Query19cFindOptJoinTree:
                    map.put(c.val, Query19cFindOptJoinTree.class);
                    break;
                case Query19dFindOptJoinTree:
                    map.put(c.val, Query19dFindOptJoinTree.class);
                    break;
                case Query1aFindOptJoinTree:
                    map.put(c.val, Query1aFindOptJoinTree.class);
                    break;
                case Query1bFindOptJoinTree:
                    map.put(c.val, Query1bFindOptJoinTree.class);
                    break;
                case Query1cFindOptJoinTree:
                    map.put(c.val, Query1cFindOptJoinTree.class);
                    break;
                case Query1dFindOptJoinTree:
                    map.put(c.val, Query1dFindOptJoinTree.class);
                    break;
                case Query20aFindOptJoinTree:
                    map.put(c.val, Query20aFindOptJoinTree.class);
                    break;
                case Query20bFindOptJoinTree:
                    map.put(c.val, Query20bFindOptJoinTree.class);
                    break;
                case Query20cFindOptJoinTree:
                    map.put(c.val, Query20cFindOptJoinTree.class);
                    break;
                case Query21aFindOptJoinTree:
                    map.put(c.val, Query21aFindOptJoinTree.class);
                    break;
                case Query21bFindOptJoinTree:
                    map.put(c.val, Query21bFindOptJoinTree.class);
                    break;
                case Query21cFindOptJoinTree:
                    map.put(c.val, Query21cFindOptJoinTree.class);
                    break;
                case Query22aFindOptJoinTree:
                    map.put(c.val, Query22aFindOptJoinTree.class);
                    break;
                case Query22bFindOptJoinTree:
                    map.put(c.val, Query22bFindOptJoinTree.class);
                    break;
                case Query22cFindOptJoinTree:
                    map.put(c.val, Query22cFindOptJoinTree.class);
                    break;
                case Query22dFindOptJoinTree:
                    map.put(c.val, Query22dFindOptJoinTree.class);
                    break;
                case Query23aFindOptJoinTree:
                    map.put(c.val, Query23aFindOptJoinTree.class);
                    break;
                case Query23bFindOptJoinTree:
                    map.put(c.val, Query23bFindOptJoinTree.class);
                    break;
                case Query23cFindOptJoinTree:
                    map.put(c.val, Query23cFindOptJoinTree.class);
                    break;
                case Query24aFindOptJoinTree:
                    map.put(c.val, Query24aFindOptJoinTree.class);
                    break;
                case Query24bFindOptJoinTree:
                    map.put(c.val, Query24bFindOptJoinTree.class);
                    break;
                case Query25aFindOptJoinTree:
                    map.put(c.val, Query25aFindOptJoinTree.class);
                    break;
                case Query25bFindOptJoinTree:
                    map.put(c.val, Query25bFindOptJoinTree.class);
                    break;
                case Query25cFindOptJoinTree:
                    map.put(c.val, Query25cFindOptJoinTree.class);
                    break;
                case Query26aFindOptJoinTree:
                    map.put(c.val, Query26aFindOptJoinTree.class);
                    break;
                case Query26bFindOptJoinTree:
                    map.put(c.val, Query26bFindOptJoinTree.class);
                    break;
                case Query26cFindOptJoinTree:
                    map.put(c.val, Query26cFindOptJoinTree.class);
                    break;
                case Query27aFindOptJoinTree:
                    map.put(c.val, Query27aFindOptJoinTree.class);
                    break;
                case Query27bFindOptJoinTree:
                    map.put(c.val, Query27bFindOptJoinTree.class);
                    break;
                case Query27cFindOptJoinTree:
                    map.put(c.val, Query27cFindOptJoinTree.class);
                    break;
                case Query28aFindOptJoinTree:
                    map.put(c.val, Query28aFindOptJoinTree.class);
                    break;
                case Query28bFindOptJoinTree:
                    map.put(c.val, Query28bFindOptJoinTree.class);
                    break;
                case Query28cFindOptJoinTree:
                    map.put(c.val, Query28cFindOptJoinTree.class);
                    break;
                case Query29aFindOptJoinTree:
                    map.put(c.val, Query29aFindOptJoinTree.class);
                    break;
                case Query29bFindOptJoinTree:
                    map.put(c.val, Query29bFindOptJoinTree.class);
                    break;
                case Query29cFindOptJoinTree:
                    map.put(c.val, Query29cFindOptJoinTree.class);
                    break;
                case Query2aFindOptJoinTree:
                    map.put(c.val, Query2aFindOptJoinTree.class);
                    break;
                case Query2bFindOptJoinTree:
                    map.put(c.val, Query2bFindOptJoinTree.class);
                    break;
                case Query2cFindOptJoinTree:
                    map.put(c.val, Query2cFindOptJoinTree.class);
                    break;
                case Query2dFindOptJoinTree:
                    map.put(c.val, Query2dFindOptJoinTree.class);
                    break;
                case Query30aFindOptJoinTree:
                    map.put(c.val, Query30aFindOptJoinTree.class);
                    break;
                case Query30bFindOptJoinTree:
                    map.put(c.val, Query30bFindOptJoinTree.class);
                    break;
                case Query30cFindOptJoinTree:
                    map.put(c.val, Query30cFindOptJoinTree.class);
                    break;
                case Query31aFindOptJoinTree:
                    map.put(c.val, Query31aFindOptJoinTree.class);
                    break;
                case Query31bFindOptJoinTree:
                    map.put(c.val, Query31bFindOptJoinTree.class);
                    break;
                case Query31cFindOptJoinTree:
                    map.put(c.val, Query31cFindOptJoinTree.class);
                    break;
                case Query32aFindOptJoinTree:
                    map.put(c.val, Query32aFindOptJoinTree.class);
                    break;
                case Query32bFindOptJoinTree:
                    map.put(c.val, Query32bFindOptJoinTree.class);
                    break;
                case Query33aFindOptJoinTree:
                    map.put(c.val, Query33aFindOptJoinTree.class);
                    break;
                case Query33bFindOptJoinTree:
                    map.put(c.val, Query33bFindOptJoinTree.class);
                    break;
                case Query33cFindOptJoinTree:
                    map.put(c.val, Query33cFindOptJoinTree.class);
                    break;
                case Query3aFindOptJoinTree:
                    map.put(c.val, Query3aFindOptJoinTree.class);
                    break;
                case Query3bFindOptJoinTree:
                    map.put(c.val, Query3bFindOptJoinTree.class);
                    break;
                case Query3cFindOptJoinTree:
                    map.put(c.val, Query3cFindOptJoinTree.class);
                    break;
                case Query4aFindOptJoinTree:
                    map.put(c.val, Query4aFindOptJoinTree.class);
                    break;
                case Query4bFindOptJoinTree:
                    map.put(c.val, Query4bFindOptJoinTree.class);
                    break;
                case Query4cFindOptJoinTree:
                    map.put(c.val, Query4cFindOptJoinTree.class);
                    break;
                case Query5aFindOptJoinTree:
                    map.put(c.val, Query5aFindOptJoinTree.class);
                    break;
                case Query5bFindOptJoinTree:
                    map.put(c.val, Query5bFindOptJoinTree.class);
                    break;
                case Query5cFindOptJoinTree:
                    map.put(c.val, Query5cFindOptJoinTree.class);
                    break;
                case Query6aFindOptJoinTree:
                    map.put(c.val, Query6aFindOptJoinTree.class);
                    break;
                case Query6bFindOptJoinTree:
                    map.put(c.val, Query6bFindOptJoinTree.class);
                    break;
                case Query6cFindOptJoinTree:
                    map.put(c.val, Query6cFindOptJoinTree.class);
                    break;
                case Query6dFindOptJoinTree:
                    map.put(c.val, Query6dFindOptJoinTree.class);
                    break;
                case Query6eFindOptJoinTree:
                    map.put(c.val, Query6eFindOptJoinTree.class);
                    break;
                case Query6fFindOptJoinTree:
                    map.put(c.val, Query6fFindOptJoinTree.class);
                    break;
                case Query7aFindOptJoinTree:
                    map.put(c.val, Query7aFindOptJoinTree.class);
                    break;
                case Query7bFindOptJoinTree:
                    map.put(c.val, Query7bFindOptJoinTree.class);
                    break;
                case Query7cFindOptJoinTree:
                    map.put(c.val, Query7cFindOptJoinTree.class);
                    break;
                case Query8aFindOptJoinTree:
                    map.put(c.val, Query8aFindOptJoinTree.class);
                    break;
                case Query8bFindOptJoinTree:
                    map.put(c.val, Query8bFindOptJoinTree.class);
                    break;
                case Query8cFindOptJoinTree:
                    map.put(c.val, Query8cFindOptJoinTree.class);
                    break;
                case Query8dFindOptJoinTree:
                    map.put(c.val, Query8dFindOptJoinTree.class);
                    break;
                case Query9aFindOptJoinTree:
                    map.put(c.val, Query9aFindOptJoinTree.class);
                    break;
                case Query9bFindOptJoinTree:
                    map.put(c.val, Query9bFindOptJoinTree.class);
                    break;
                case Query9cFindOptJoinTree:
                    map.put(c.val, Query9cFindOptJoinTree.class);
                    break;
                case Query9dFindOptJoinTree:
                    map.put(c.val, Query9dFindOptJoinTree.class);
                    break;
            }
        }
    }

    private final String val;

    private JOBQueriesFindOptJoinTree(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
