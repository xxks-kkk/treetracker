package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q1.Query1aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17eOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17fOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q24.Query24aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q24.Query24bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q32.Query32aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q32.Query32bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5aOptJoinTreeOptOrdering2;
import org.zhu45.treetracker.benchmark.job.q5.Query5bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6eOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6fOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8dOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9aOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9bOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9cOptJoinTreeOptOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9dOptJoinTreeOptOrdering;

import java.util.HashMap;
import java.util.Map;

/**
 * Generated by GenerateJOBBenchmarkWithOptimalJoinTrees
 */
public enum JOBQueriesAutoGen
        implements QueryEnum
{
    Query10aOptJoinTreeOptOrdering("Query10aOptJoinTreeOptOrdering"),
    Query10bOptJoinTreeOptOrdering("Query10bOptJoinTreeOptOrdering"),
    Query10cOptJoinTreeOptOrdering("Query10cOptJoinTreeOptOrdering"),
    Query11aOptJoinTreeOptOrdering("Query11aOptJoinTreeOptOrdering"),
    Query11bOptJoinTreeOptOrdering("Query11bOptJoinTreeOptOrdering"),
    Query11cOptJoinTreeOptOrdering("Query11cOptJoinTreeOptOrdering"),
    Query11dOptJoinTreeOptOrdering("Query11dOptJoinTreeOptOrdering"),
    Query12aOptJoinTreeOptOrdering("Query12aOptJoinTreeOptOrdering"),
    Query12bOptJoinTreeOptOrdering("Query12bOptJoinTreeOptOrdering"),
    Query12cOptJoinTreeOptOrdering("Query12cOptJoinTreeOptOrdering"),
    Query13aOptJoinTreeOptOrdering("Query13aOptJoinTreeOptOrdering"),
    Query13bOptJoinTreeOptOrdering("Query13bOptJoinTreeOptOrdering"),
    Query13cOptJoinTreeOptOrdering("Query13cOptJoinTreeOptOrdering"),
    Query13dOptJoinTreeOptOrdering("Query13dOptJoinTreeOptOrdering"),
    Query14aOptJoinTreeOptOrdering("Query14aOptJoinTreeOptOrdering"),
    Query14bOptJoinTreeOptOrdering("Query14bOptJoinTreeOptOrdering"),
    Query14cOptJoinTreeOptOrdering("Query14cOptJoinTreeOptOrdering"),
    Query15aOptJoinTreeOptOrdering("Query15aOptJoinTreeOptOrdering"),
    Query15bOptJoinTreeOptOrdering("Query15bOptJoinTreeOptOrdering"),
    Query15cOptJoinTreeOptOrdering("Query15cOptJoinTreeOptOrdering"),
    Query15dOptJoinTreeOptOrdering("Query15dOptJoinTreeOptOrdering"),
    Query16aOptJoinTreeOptOrdering("Query16aOptJoinTreeOptOrdering"),
    Query16bOptJoinTreeOptOrdering("Query16bOptJoinTreeOptOrdering"),
    Query16cOptJoinTreeOptOrdering("Query16cOptJoinTreeOptOrdering"),
    Query16dOptJoinTreeOptOrdering("Query16dOptJoinTreeOptOrdering"),
    Query17aOptJoinTreeOptOrdering("Query17aOptJoinTreeOptOrdering"),
    Query17bOptJoinTreeOptOrdering("Query17bOptJoinTreeOptOrdering"),
    Query17cOptJoinTreeOptOrdering("Query17cOptJoinTreeOptOrdering"),
    Query17dOptJoinTreeOptOrdering("Query17dOptJoinTreeOptOrdering"),
    Query17eOptJoinTreeOptOrdering("Query17eOptJoinTreeOptOrdering"),
    Query17fOptJoinTreeOptOrdering("Query17fOptJoinTreeOptOrdering"),
    Query18aOptJoinTreeOptOrdering("Query18aOptJoinTreeOptOrdering"),
    Query18bOptJoinTreeOptOrdering("Query18bOptJoinTreeOptOrdering"),
    Query18cOptJoinTreeOptOrdering("Query18cOptJoinTreeOptOrdering"),
    Query19aOptJoinTreeOptOrdering("Query19aOptJoinTreeOptOrdering"),
    Query19bOptJoinTreeOptOrdering("Query19bOptJoinTreeOptOrdering"),
    Query19cOptJoinTreeOptOrdering("Query19cOptJoinTreeOptOrdering"),
    Query19dOptJoinTreeOptOrdering("Query19dOptJoinTreeOptOrdering"),
    Query1aOptJoinTreeOptOrdering("Query1aOptJoinTreeOptOrdering"),
    Query1bOptJoinTreeOptOrdering("Query1bOptJoinTreeOptOrdering"),
    Query1cOptJoinTreeOptOrdering("Query1cOptJoinTreeOptOrdering"),
    Query1dOptJoinTreeOptOrdering("Query1dOptJoinTreeOptOrdering"),
    Query20aOptJoinTreeOptOrdering("Query20aOptJoinTreeOptOrdering"),
    Query20bOptJoinTreeOptOrdering("Query20bOptJoinTreeOptOrdering"),
    Query20cOptJoinTreeOptOrdering("Query20cOptJoinTreeOptOrdering"),
    Query21aOptJoinTreeOptOrdering("Query21aOptJoinTreeOptOrdering"),
    Query21bOptJoinTreeOptOrdering("Query21bOptJoinTreeOptOrdering"),
    Query21cOptJoinTreeOptOrdering("Query21cOptJoinTreeOptOrdering"),
    Query22aOptJoinTreeOptOrdering("Query22aOptJoinTreeOptOrdering"),
    Query22bOptJoinTreeOptOrdering("Query22bOptJoinTreeOptOrdering"),
    Query22cOptJoinTreeOptOrdering("Query22cOptJoinTreeOptOrdering"),
    Query22dOptJoinTreeOptOrdering("Query22dOptJoinTreeOptOrdering"),
    Query23aOptJoinTreeOptOrdering("Query23aOptJoinTreeOptOrdering"),
    Query23bOptJoinTreeOptOrdering("Query23bOptJoinTreeOptOrdering"),
    Query23cOptJoinTreeOptOrdering("Query23cOptJoinTreeOptOrdering"),
    Query24aOptJoinTreeOptOrdering("Query24aOptJoinTreeOptOrdering"),
    Query24bOptJoinTreeOptOrdering("Query24bOptJoinTreeOptOrdering"),
    Query25aOptJoinTreeOptOrdering("Query25aOptJoinTreeOptOrdering"),
    Query25bOptJoinTreeOptOrdering("Query25bOptJoinTreeOptOrdering"),
    Query25cOptJoinTreeOptOrdering("Query25cOptJoinTreeOptOrdering"),
    Query26aOptJoinTreeOptOrdering("Query26aOptJoinTreeOptOrdering"),
    Query26bOptJoinTreeOptOrdering("Query26bOptJoinTreeOptOrdering"),
    Query26cOptJoinTreeOptOrdering("Query26cOptJoinTreeOptOrdering"),
    Query27aOptJoinTreeOptOrdering("Query27aOptJoinTreeOptOrdering"),
    Query27bOptJoinTreeOptOrdering("Query27bOptJoinTreeOptOrdering"),
    Query27cOptJoinTreeOptOrdering("Query27cOptJoinTreeOptOrdering"),
    Query28aOptJoinTreeOptOrdering("Query28aOptJoinTreeOptOrdering"),
    Query28bOptJoinTreeOptOrdering("Query28bOptJoinTreeOptOrdering"),
    Query28cOptJoinTreeOptOrdering("Query28cOptJoinTreeOptOrdering"),
    Query29aOptJoinTreeOptOrdering("Query29aOptJoinTreeOptOrdering"),
    Query29bOptJoinTreeOptOrdering("Query29bOptJoinTreeOptOrdering"),
    Query29cOptJoinTreeOptOrdering("Query29cOptJoinTreeOptOrdering"),
    Query2aOptJoinTreeOptOrdering("Query2aOptJoinTreeOptOrdering"),
    Query2bOptJoinTreeOptOrdering("Query2bOptJoinTreeOptOrdering"),
    Query2cOptJoinTreeOptOrdering("Query2cOptJoinTreeOptOrdering"),
    Query2dOptJoinTreeOptOrdering("Query2dOptJoinTreeOptOrdering"),
    Query30aOptJoinTreeOptOrdering("Query30aOptJoinTreeOptOrdering"),
    Query30bOptJoinTreeOptOrdering("Query30bOptJoinTreeOptOrdering"),
    Query30cOptJoinTreeOptOrdering("Query30cOptJoinTreeOptOrdering"),
    Query31aOptJoinTreeOptOrdering("Query31aOptJoinTreeOptOrdering"),
    Query31bOptJoinTreeOptOrdering("Query31bOptJoinTreeOptOrdering"),
    Query31cOptJoinTreeOptOrdering("Query31cOptJoinTreeOptOrdering"),
    Query32aOptJoinTreeOptOrdering("Query32aOptJoinTreeOptOrdering"),
    Query32bOptJoinTreeOptOrdering("Query32bOptJoinTreeOptOrdering"),
    Query33aOptJoinTreeOptOrdering("Query33aOptJoinTreeOptOrdering"),
    Query33bOptJoinTreeOptOrdering("Query33bOptJoinTreeOptOrdering"),
    Query33cOptJoinTreeOptOrdering("Query33cOptJoinTreeOptOrdering"),
    Query3aOptJoinTreeOptOrdering("Query3aOptJoinTreeOptOrdering"),
    Query3bOptJoinTreeOptOrdering("Query3bOptJoinTreeOptOrdering"),
    Query3cOptJoinTreeOptOrdering("Query3cOptJoinTreeOptOrdering"),
    Query4aOptJoinTreeOptOrdering("Query4aOptJoinTreeOptOrdering"),
    Query4bOptJoinTreeOptOrdering("Query4bOptJoinTreeOptOrdering"),
    Query4cOptJoinTreeOptOrdering("Query4cOptJoinTreeOptOrdering"),
    Query5aOptJoinTreeOptOrdering("Query5aOptJoinTreeOptOrdering"),
    Query5aOptJoinTreeOptOrdering2("Query5aOptJoinTreeOptOrdering2"),
    Query5bOptJoinTreeOptOrdering("Query5bOptJoinTreeOptOrdering"),
    Query5cOptJoinTreeOptOrdering("Query5cOptJoinTreeOptOrdering"),
    Query6aOptJoinTreeOptOrdering("Query6aOptJoinTreeOptOrdering"),
    Query6bOptJoinTreeOptOrdering("Query6bOptJoinTreeOptOrdering"),
    Query6cOptJoinTreeOptOrdering("Query6cOptJoinTreeOptOrdering"),
    Query6dOptJoinTreeOptOrdering("Query6dOptJoinTreeOptOrdering"),
    Query6eOptJoinTreeOptOrdering("Query6eOptJoinTreeOptOrdering"),
    Query6fOptJoinTreeOptOrdering("Query6fOptJoinTreeOptOrdering"),
    Query7aOptJoinTreeOptOrdering("Query7aOptJoinTreeOptOrdering"),
    Query7bOptJoinTreeOptOrdering("Query7bOptJoinTreeOptOrdering"),
    Query7cOptJoinTreeOptOrdering("Query7cOptJoinTreeOptOrdering"),
    Query8aOptJoinTreeOptOrdering("Query8aOptJoinTreeOptOrdering"),
    Query8bOptJoinTreeOptOrdering("Query8bOptJoinTreeOptOrdering"),
    Query8cOptJoinTreeOptOrdering("Query8cOptJoinTreeOptOrdering"),
    Query8dOptJoinTreeOptOrdering("Query8dOptJoinTreeOptOrdering"),
    Query9aOptJoinTreeOptOrdering("Query9aOptJoinTreeOptOrdering"),
    Query9bOptJoinTreeOptOrdering("Query9bOptJoinTreeOptOrdering"),
    Query9cOptJoinTreeOptOrdering("Query9cOptJoinTreeOptOrdering"),
    Query9dOptJoinTreeOptOrdering("Query9dOptJoinTreeOptOrdering");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (JOBQueriesAutoGen c : values()) {
            switch (c) {
                case Query10aOptJoinTreeOptOrdering:
                    map.put(c.val, Query10aOptJoinTreeOptOrdering.class);
                    break;
                case Query10bOptJoinTreeOptOrdering:
                    map.put(c.val, Query10bOptJoinTreeOptOrdering.class);
                    break;
                case Query10cOptJoinTreeOptOrdering:
                    map.put(c.val, Query10cOptJoinTreeOptOrdering.class);
                    break;
                case Query11aOptJoinTreeOptOrdering:
                    map.put(c.val, Query11aOptJoinTreeOptOrdering.class);
                    break;
                case Query11bOptJoinTreeOptOrdering:
                    map.put(c.val, Query11bOptJoinTreeOptOrdering.class);
                    break;
                case Query11cOptJoinTreeOptOrdering:
                    map.put(c.val, Query11cOptJoinTreeOptOrdering.class);
                    break;
                case Query11dOptJoinTreeOptOrdering:
                    map.put(c.val, Query11dOptJoinTreeOptOrdering.class);
                    break;
                case Query12aOptJoinTreeOptOrdering:
                    map.put(c.val, Query12aOptJoinTreeOptOrdering.class);
                    break;
                case Query12bOptJoinTreeOptOrdering:
                    map.put(c.val, Query12bOptJoinTreeOptOrdering.class);
                    break;
                case Query12cOptJoinTreeOptOrdering:
                    map.put(c.val, Query12cOptJoinTreeOptOrdering.class);
                    break;
                case Query13aOptJoinTreeOptOrdering:
                    map.put(c.val, Query13aOptJoinTreeOptOrdering.class);
                    break;
                case Query13bOptJoinTreeOptOrdering:
                    map.put(c.val, Query13bOptJoinTreeOptOrdering.class);
                    break;
                case Query13cOptJoinTreeOptOrdering:
                    map.put(c.val, Query13cOptJoinTreeOptOrdering.class);
                    break;
                case Query13dOptJoinTreeOptOrdering:
                    map.put(c.val, Query13dOptJoinTreeOptOrdering.class);
                    break;
                case Query14aOptJoinTreeOptOrdering:
                    map.put(c.val, Query14aOptJoinTreeOptOrdering.class);
                    break;
                case Query14bOptJoinTreeOptOrdering:
                    map.put(c.val, Query14bOptJoinTreeOptOrdering.class);
                    break;
                case Query14cOptJoinTreeOptOrdering:
                    map.put(c.val, Query14cOptJoinTreeOptOrdering.class);
                    break;
                case Query15aOptJoinTreeOptOrdering:
                    map.put(c.val, Query15aOptJoinTreeOptOrdering.class);
                    break;
                case Query15bOptJoinTreeOptOrdering:
                    map.put(c.val, Query15bOptJoinTreeOptOrdering.class);
                    break;
                case Query15cOptJoinTreeOptOrdering:
                    map.put(c.val, Query15cOptJoinTreeOptOrdering.class);
                    break;
                case Query15dOptJoinTreeOptOrdering:
                    map.put(c.val, Query15dOptJoinTreeOptOrdering.class);
                    break;
                case Query16aOptJoinTreeOptOrdering:
                    map.put(c.val, Query16aOptJoinTreeOptOrdering.class);
                    break;
                case Query16bOptJoinTreeOptOrdering:
                    map.put(c.val, Query16bOptJoinTreeOptOrdering.class);
                    break;
                case Query16cOptJoinTreeOptOrdering:
                    map.put(c.val, Query16cOptJoinTreeOptOrdering.class);
                    break;
                case Query16dOptJoinTreeOptOrdering:
                    map.put(c.val, Query16dOptJoinTreeOptOrdering.class);
                    break;
                case Query17aOptJoinTreeOptOrdering:
                    map.put(c.val, Query17aOptJoinTreeOptOrdering.class);
                    break;
                case Query17bOptJoinTreeOptOrdering:
                    map.put(c.val, Query17bOptJoinTreeOptOrdering.class);
                    break;
                case Query17cOptJoinTreeOptOrdering:
                    map.put(c.val, Query17cOptJoinTreeOptOrdering.class);
                    break;
                case Query17dOptJoinTreeOptOrdering:
                    map.put(c.val, Query17dOptJoinTreeOptOrdering.class);
                    break;
                case Query17eOptJoinTreeOptOrdering:
                    map.put(c.val, Query17eOptJoinTreeOptOrdering.class);
                    break;
                case Query17fOptJoinTreeOptOrdering:
                    map.put(c.val, Query17fOptJoinTreeOptOrdering.class);
                    break;
                case Query18aOptJoinTreeOptOrdering:
                    map.put(c.val, Query18aOptJoinTreeOptOrdering.class);
                    break;
                case Query18bOptJoinTreeOptOrdering:
                    map.put(c.val, Query18bOptJoinTreeOptOrdering.class);
                    break;
                case Query18cOptJoinTreeOptOrdering:
                    map.put(c.val, Query18cOptJoinTreeOptOrdering.class);
                    break;
                case Query19aOptJoinTreeOptOrdering:
                    map.put(c.val, Query19aOptJoinTreeOptOrdering.class);
                    break;
                case Query19bOptJoinTreeOptOrdering:
                    map.put(c.val, Query19bOptJoinTreeOptOrdering.class);
                    break;
                case Query19cOptJoinTreeOptOrdering:
                    map.put(c.val, Query19cOptJoinTreeOptOrdering.class);
                    break;
                case Query19dOptJoinTreeOptOrdering:
                    map.put(c.val, Query19dOptJoinTreeOptOrdering.class);
                    break;
                case Query1aOptJoinTreeOptOrdering:
                    map.put(c.val, Query1aOptJoinTreeOptOrdering.class);
                    break;
                case Query1bOptJoinTreeOptOrdering:
                    map.put(c.val, Query1bOptJoinTreeOptOrdering.class);
                    break;
                case Query1cOptJoinTreeOptOrdering:
                    map.put(c.val, Query1cOptJoinTreeOptOrdering.class);
                    break;
                case Query1dOptJoinTreeOptOrdering:
                    map.put(c.val, Query1dOptJoinTreeOptOrdering.class);
                    break;
                case Query20aOptJoinTreeOptOrdering:
                    map.put(c.val, Query20aOptJoinTreeOptOrdering.class);
                    break;
                case Query20bOptJoinTreeOptOrdering:
                    map.put(c.val, Query20bOptJoinTreeOptOrdering.class);
                    break;
                case Query20cOptJoinTreeOptOrdering:
                    map.put(c.val, Query20cOptJoinTreeOptOrdering.class);
                    break;
                case Query21aOptJoinTreeOptOrdering:
                    map.put(c.val, Query21aOptJoinTreeOptOrdering.class);
                    break;
                case Query21bOptJoinTreeOptOrdering:
                    map.put(c.val, Query21bOptJoinTreeOptOrdering.class);
                    break;
                case Query21cOptJoinTreeOptOrdering:
                    map.put(c.val, Query21cOptJoinTreeOptOrdering.class);
                    break;
                case Query22aOptJoinTreeOptOrdering:
                    map.put(c.val, Query22aOptJoinTreeOptOrdering.class);
                    break;
                case Query22bOptJoinTreeOptOrdering:
                    map.put(c.val, Query22bOptJoinTreeOptOrdering.class);
                    break;
                case Query22cOptJoinTreeOptOrdering:
                    map.put(c.val, Query22cOptJoinTreeOptOrdering.class);
                    break;
                case Query22dOptJoinTreeOptOrdering:
                    map.put(c.val, Query22dOptJoinTreeOptOrdering.class);
                    break;
                case Query23aOptJoinTreeOptOrdering:
                    map.put(c.val, Query23aOptJoinTreeOptOrdering.class);
                    break;
                case Query23bOptJoinTreeOptOrdering:
                    map.put(c.val, Query23bOptJoinTreeOptOrdering.class);
                    break;
                case Query23cOptJoinTreeOptOrdering:
                    map.put(c.val, Query23cOptJoinTreeOptOrdering.class);
                    break;
                case Query24aOptJoinTreeOptOrdering:
                    map.put(c.val, Query24aOptJoinTreeOptOrdering.class);
                    break;
                case Query24bOptJoinTreeOptOrdering:
                    map.put(c.val, Query24bOptJoinTreeOptOrdering.class);
                    break;
                case Query25aOptJoinTreeOptOrdering:
                    map.put(c.val, Query25aOptJoinTreeOptOrdering.class);
                    break;
                case Query25bOptJoinTreeOptOrdering:
                    map.put(c.val, Query25bOptJoinTreeOptOrdering.class);
                    break;
                case Query25cOptJoinTreeOptOrdering:
                    map.put(c.val, Query25cOptJoinTreeOptOrdering.class);
                    break;
                case Query26aOptJoinTreeOptOrdering:
                    map.put(c.val, Query26aOptJoinTreeOptOrdering.class);
                    break;
                case Query26bOptJoinTreeOptOrdering:
                    map.put(c.val, Query26bOptJoinTreeOptOrdering.class);
                    break;
                case Query26cOptJoinTreeOptOrdering:
                    map.put(c.val, Query26cOptJoinTreeOptOrdering.class);
                    break;
                case Query27aOptJoinTreeOptOrdering:
                    map.put(c.val, Query27aOptJoinTreeOptOrdering.class);
                    break;
                case Query27bOptJoinTreeOptOrdering:
                    map.put(c.val, Query27bOptJoinTreeOptOrdering.class);
                    break;
                case Query27cOptJoinTreeOptOrdering:
                    map.put(c.val, Query27cOptJoinTreeOptOrdering.class);
                    break;
                case Query28aOptJoinTreeOptOrdering:
                    map.put(c.val, Query28aOptJoinTreeOptOrdering.class);
                    break;
                case Query28bOptJoinTreeOptOrdering:
                    map.put(c.val, Query28bOptJoinTreeOptOrdering.class);
                    break;
                case Query28cOptJoinTreeOptOrdering:
                    map.put(c.val, Query28cOptJoinTreeOptOrdering.class);
                    break;
                case Query29aOptJoinTreeOptOrdering:
                    map.put(c.val, Query29aOptJoinTreeOptOrdering.class);
                    break;
                case Query29bOptJoinTreeOptOrdering:
                    map.put(c.val, Query29bOptJoinTreeOptOrdering.class);
                    break;
                case Query29cOptJoinTreeOptOrdering:
                    map.put(c.val, Query29cOptJoinTreeOptOrdering.class);
                    break;
                case Query2aOptJoinTreeOptOrdering:
                    map.put(c.val, Query2aOptJoinTreeOptOrdering.class);
                    break;
                case Query2bOptJoinTreeOptOrdering:
                    map.put(c.val, Query2bOptJoinTreeOptOrdering.class);
                    break;
                case Query2cOptJoinTreeOptOrdering:
                    map.put(c.val, Query2cOptJoinTreeOptOrdering.class);
                    break;
                case Query2dOptJoinTreeOptOrdering:
                    map.put(c.val, Query2dOptJoinTreeOptOrdering.class);
                    break;
                case Query30aOptJoinTreeOptOrdering:
                    map.put(c.val, Query30aOptJoinTreeOptOrdering.class);
                    break;
                case Query30bOptJoinTreeOptOrdering:
                    map.put(c.val, Query30bOptJoinTreeOptOrdering.class);
                    break;
                case Query30cOptJoinTreeOptOrdering:
                    map.put(c.val, Query30cOptJoinTreeOptOrdering.class);
                    break;
                case Query31aOptJoinTreeOptOrdering:
                    map.put(c.val, Query31aOptJoinTreeOptOrdering.class);
                    break;
                case Query31bOptJoinTreeOptOrdering:
                    map.put(c.val, Query31bOptJoinTreeOptOrdering.class);
                    break;
                case Query31cOptJoinTreeOptOrdering:
                    map.put(c.val, Query31cOptJoinTreeOptOrdering.class);
                    break;
                case Query32aOptJoinTreeOptOrdering:
                    map.put(c.val, Query32aOptJoinTreeOptOrdering.class);
                    break;
                case Query32bOptJoinTreeOptOrdering:
                    map.put(c.val, Query32bOptJoinTreeOptOrdering.class);
                    break;
                case Query33aOptJoinTreeOptOrdering:
                    map.put(c.val, Query33aOptJoinTreeOptOrdering.class);
                    break;
                case Query33bOptJoinTreeOptOrdering:
                    map.put(c.val, Query33bOptJoinTreeOptOrdering.class);
                    break;
                case Query33cOptJoinTreeOptOrdering:
                    map.put(c.val, Query33cOptJoinTreeOptOrdering.class);
                    break;
                case Query3aOptJoinTreeOptOrdering:
                    map.put(c.val, Query3aOptJoinTreeOptOrdering.class);
                    break;
                case Query3bOptJoinTreeOptOrdering:
                    map.put(c.val, Query3bOptJoinTreeOptOrdering.class);
                    break;
                case Query3cOptJoinTreeOptOrdering:
                    map.put(c.val, Query3cOptJoinTreeOptOrdering.class);
                    break;
                case Query4aOptJoinTreeOptOrdering:
                    map.put(c.val, Query4aOptJoinTreeOptOrdering.class);
                    break;
                case Query4bOptJoinTreeOptOrdering:
                    map.put(c.val, Query4bOptJoinTreeOptOrdering.class);
                    break;
                case Query4cOptJoinTreeOptOrdering:
                    map.put(c.val, Query4cOptJoinTreeOptOrdering.class);
                    break;
                case Query5aOptJoinTreeOptOrdering:
                    map.put(c.val, Query5aOptJoinTreeOptOrdering.class);
                    break;
                case Query5aOptJoinTreeOptOrdering2:
                    map.put(c.val, Query5aOptJoinTreeOptOrdering2.class);
                    break;
                case Query5bOptJoinTreeOptOrdering:
                    map.put(c.val, Query5bOptJoinTreeOptOrdering.class);
                    break;
                case Query5cOptJoinTreeOptOrdering:
                    map.put(c.val, Query5cOptJoinTreeOptOrdering.class);
                    break;
                case Query6aOptJoinTreeOptOrdering:
                    map.put(c.val, Query6aOptJoinTreeOptOrdering.class);
                    break;
                case Query6bOptJoinTreeOptOrdering:
                    map.put(c.val, Query6bOptJoinTreeOptOrdering.class);
                    break;
                case Query6cOptJoinTreeOptOrdering:
                    map.put(c.val, Query6cOptJoinTreeOptOrdering.class);
                    break;
                case Query6dOptJoinTreeOptOrdering:
                    map.put(c.val, Query6dOptJoinTreeOptOrdering.class);
                    break;
                case Query6eOptJoinTreeOptOrdering:
                    map.put(c.val, Query6eOptJoinTreeOptOrdering.class);
                    break;
                case Query6fOptJoinTreeOptOrdering:
                    map.put(c.val, Query6fOptJoinTreeOptOrdering.class);
                    break;
                case Query7aOptJoinTreeOptOrdering:
                    map.put(c.val, Query7aOptJoinTreeOptOrdering.class);
                    break;
                case Query7bOptJoinTreeOptOrdering:
                    map.put(c.val, Query7bOptJoinTreeOptOrdering.class);
                    break;
                case Query7cOptJoinTreeOptOrdering:
                    map.put(c.val, Query7cOptJoinTreeOptOrdering.class);
                    break;
                case Query8aOptJoinTreeOptOrdering:
                    map.put(c.val, Query8aOptJoinTreeOptOrdering.class);
                    break;
                case Query8bOptJoinTreeOptOrdering:
                    map.put(c.val, Query8bOptJoinTreeOptOrdering.class);
                    break;
                case Query8cOptJoinTreeOptOrdering:
                    map.put(c.val, Query8cOptJoinTreeOptOrdering.class);
                    break;
                case Query8dOptJoinTreeOptOrdering:
                    map.put(c.val, Query8dOptJoinTreeOptOrdering.class);
                    break;
                case Query9aOptJoinTreeOptOrdering:
                    map.put(c.val, Query9aOptJoinTreeOptOrdering.class);
                    break;
                case Query9bOptJoinTreeOptOrdering:
                    map.put(c.val, Query9bOptJoinTreeOptOrdering.class);
                    break;
                case Query9cOptJoinTreeOptOrdering:
                    map.put(c.val, Query9cOptJoinTreeOptOrdering.class);
                    break;
                case Query9dOptJoinTreeOptOrdering:
                    map.put(c.val, Query9dOptJoinTreeOptOrdering.class);
                    break;
            }
        }
    }

    private final String val;

    private JOBQueriesAutoGen(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
