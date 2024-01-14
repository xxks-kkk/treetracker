package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q1.Query1aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q1.Query1cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q1.Query1dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q10.Query10aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q10.Query10bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q10.Query10cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q11.Query11aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q11.Query11bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q11.Query11cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q11.Query11dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q12.Query12aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q12.Query12bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q12.Query12cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q13.Query13aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q13.Query13bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q13.Query13cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q13.Query13dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q14.Query14aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q14.Query14bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q14.Query14cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q15.Query15aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q15.Query15bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q15.Query15cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q15.Query15dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q16.Query16aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q16.Query16bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q16.Query16cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q16.Query16dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q17.Query17aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q17.Query17bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q17.Query17cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q17.Query17dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q17.Query17eOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q17.Query17fOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q18.Query18aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q18.Query18bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q18.Query18cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q19.Query19aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q19.Query19bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q19.Query19cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q19.Query19dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q2.Query2aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q2.Query2bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q2.Query2dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q20.Query20aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q20.Query20bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q20.Query20cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q21.Query21aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q21.Query21bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q21.Query21cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q22.Query22aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q22.Query22bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q22.Query22cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q22.Query22dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q23.Query23aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q23.Query23bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q23.Query23cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q24.Query24aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q24.Query24bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q25.Query25aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q25.Query25bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q26.Query26aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q26.Query26bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q26.Query26cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q27.Query27aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q27.Query27bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q27.Query27cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q28.Query28aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q28.Query28bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q28.Query28cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q29.Query29aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q29.Query29bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q29.Query29cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q3.Query3aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q3.Query3cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q30.Query30aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q30.Query30bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q30.Query30cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q31.Query31aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q31.Query31bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q31.Query31cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q32.Query32aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q32.Query32bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q33.Query33aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q33.Query33bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q33.Query33cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q4.Query4bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q4.Query4cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q5.Query5aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q5.Query5bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q5.Query5cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q6.Query6aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q6.Query6bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q6.Query6cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q6.Query6dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q6.Query6eOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q6.Query6fOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q7.Query7aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q7.Query7bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q7.Query7cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q8.Query8aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q8.Query8bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q8.Query8cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q8.Query8dOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q9.Query9aOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q9.Query9bOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q9.Query9cOptJoinTreeOptOrderingYB;
import org.zhu45.treetracker.benchmark.job.q9.Query9dOptJoinTreeOptOrderingYB;

import java.util.HashMap;
import java.util.Map;

/**
 * Generated by GenerateJOBBenchmarkWithOptimalJoinTrees
 */
public enum JOBQueriesYannakakisB
        implements QueryEnum
{
    Query10aOptJoinTreeOptOrderingYB("Query10aOptJoinTreeOptOrderingYB"),
    Query10bOptJoinTreeOptOrderingYB("Query10bOptJoinTreeOptOrderingYB"),
    Query10cOptJoinTreeOptOrderingYB("Query10cOptJoinTreeOptOrderingYB"),
    Query11aOptJoinTreeOptOrderingYB("Query11aOptJoinTreeOptOrderingYB"),
    Query11bOptJoinTreeOptOrderingYB("Query11bOptJoinTreeOptOrderingYB"),
    Query11cOptJoinTreeOptOrderingYB("Query11cOptJoinTreeOptOrderingYB"),
    Query11dOptJoinTreeOptOrderingYB("Query11dOptJoinTreeOptOrderingYB"),
    Query12aOptJoinTreeOptOrderingYB("Query12aOptJoinTreeOptOrderingYB"),
    Query12bOptJoinTreeOptOrderingYB("Query12bOptJoinTreeOptOrderingYB"),
    Query12cOptJoinTreeOptOrderingYB("Query12cOptJoinTreeOptOrderingYB"),
    Query13aOptJoinTreeOptOrderingYB("Query13aOptJoinTreeOptOrderingYB"),
    Query13bOptJoinTreeOptOrderingYB("Query13bOptJoinTreeOptOrderingYB"),
    Query13cOptJoinTreeOptOrderingYB("Query13cOptJoinTreeOptOrderingYB"),
    Query13dOptJoinTreeOptOrderingYB("Query13dOptJoinTreeOptOrderingYB"),
    Query14aOptJoinTreeOptOrderingYB("Query14aOptJoinTreeOptOrderingYB"),
    Query14bOptJoinTreeOptOrderingYB("Query14bOptJoinTreeOptOrderingYB"),
    Query14cOptJoinTreeOptOrderingYB("Query14cOptJoinTreeOptOrderingYB"),
    Query15aOptJoinTreeOptOrderingYB("Query15aOptJoinTreeOptOrderingYB"),
    Query15bOptJoinTreeOptOrderingYB("Query15bOptJoinTreeOptOrderingYB"),
    Query15cOptJoinTreeOptOrderingYB("Query15cOptJoinTreeOptOrderingYB"),
    Query15dOptJoinTreeOptOrderingYB("Query15dOptJoinTreeOptOrderingYB"),
    Query16aOptJoinTreeOptOrderingYB("Query16aOptJoinTreeOptOrderingYB"),
    Query16bOptJoinTreeOptOrderingYB("Query16bOptJoinTreeOptOrderingYB"),
    Query16cOptJoinTreeOptOrderingYB("Query16cOptJoinTreeOptOrderingYB"),
    Query16dOptJoinTreeOptOrderingYB("Query16dOptJoinTreeOptOrderingYB"),
    Query17aOptJoinTreeOptOrderingYB("Query17aOptJoinTreeOptOrderingYB"),
    Query17bOptJoinTreeOptOrderingYB("Query17bOptJoinTreeOptOrderingYB"),
    Query17cOptJoinTreeOptOrderingYB("Query17cOptJoinTreeOptOrderingYB"),
    Query17dOptJoinTreeOptOrderingYB("Query17dOptJoinTreeOptOrderingYB"),
    Query17eOptJoinTreeOptOrderingYB("Query17eOptJoinTreeOptOrderingYB"),
    Query17fOptJoinTreeOptOrderingYB("Query17fOptJoinTreeOptOrderingYB"),
    Query18aOptJoinTreeOptOrderingYB("Query18aOptJoinTreeOptOrderingYB"),
    Query18bOptJoinTreeOptOrderingYB("Query18bOptJoinTreeOptOrderingYB"),
    Query18cOptJoinTreeOptOrderingYB("Query18cOptJoinTreeOptOrderingYB"),
    Query19aOptJoinTreeOptOrderingYB("Query19aOptJoinTreeOptOrderingYB"),
    Query19bOptJoinTreeOptOrderingYB("Query19bOptJoinTreeOptOrderingYB"),
    Query19cOptJoinTreeOptOrderingYB("Query19cOptJoinTreeOptOrderingYB"),
    Query19dOptJoinTreeOptOrderingYB("Query19dOptJoinTreeOptOrderingYB"),
    Query1aOptJoinTreeOptOrderingYB("Query1aOptJoinTreeOptOrderingYB"),
    Query1bOptJoinTreeOptOrderingYB("Query1bOptJoinTreeOptOrderingYB"),
    Query1cOptJoinTreeOptOrderingYB("Query1cOptJoinTreeOptOrderingYB"),
    Query1dOptJoinTreeOptOrderingYB("Query1dOptJoinTreeOptOrderingYB"),
    Query20aOptJoinTreeOptOrderingYB("Query20aOptJoinTreeOptOrderingYB"),
    Query20bOptJoinTreeOptOrderingYB("Query20bOptJoinTreeOptOrderingYB"),
    Query20cOptJoinTreeOptOrderingYB("Query20cOptJoinTreeOptOrderingYB"),
    Query21aOptJoinTreeOptOrderingYB("Query21aOptJoinTreeOptOrderingYB"),
    Query21bOptJoinTreeOptOrderingYB("Query21bOptJoinTreeOptOrderingYB"),
    Query21cOptJoinTreeOptOrderingYB("Query21cOptJoinTreeOptOrderingYB"),
    Query22aOptJoinTreeOptOrderingYB("Query22aOptJoinTreeOptOrderingYB"),
    Query22bOptJoinTreeOptOrderingYB("Query22bOptJoinTreeOptOrderingYB"),
    Query22cOptJoinTreeOptOrderingYB("Query22cOptJoinTreeOptOrderingYB"),
    Query22dOptJoinTreeOptOrderingYB("Query22dOptJoinTreeOptOrderingYB"),
    Query23aOptJoinTreeOptOrderingYB("Query23aOptJoinTreeOptOrderingYB"),
    Query23bOptJoinTreeOptOrderingYB("Query23bOptJoinTreeOptOrderingYB"),
    Query23cOptJoinTreeOptOrderingYB("Query23cOptJoinTreeOptOrderingYB"),
    Query24aOptJoinTreeOptOrderingYB("Query24aOptJoinTreeOptOrderingYB"),
    Query24bOptJoinTreeOptOrderingYB("Query24bOptJoinTreeOptOrderingYB"),
    Query25aOptJoinTreeOptOrderingYB("Query25aOptJoinTreeOptOrderingYB"),
    Query25bOptJoinTreeOptOrderingYB("Query25bOptJoinTreeOptOrderingYB"),
    Query25cOptJoinTreeOptOrderingYB("Query25cOptJoinTreeOptOrderingYB"),
    Query26aOptJoinTreeOptOrderingYB("Query26aOptJoinTreeOptOrderingYB"),
    Query26bOptJoinTreeOptOrderingYB("Query26bOptJoinTreeOptOrderingYB"),
    Query26cOptJoinTreeOptOrderingYB("Query26cOptJoinTreeOptOrderingYB"),
    Query27aOptJoinTreeOptOrderingYB("Query27aOptJoinTreeOptOrderingYB"),
    Query27bOptJoinTreeOptOrderingYB("Query27bOptJoinTreeOptOrderingYB"),
    Query27cOptJoinTreeOptOrderingYB("Query27cOptJoinTreeOptOrderingYB"),
    Query28aOptJoinTreeOptOrderingYB("Query28aOptJoinTreeOptOrderingYB"),
    Query28bOptJoinTreeOptOrderingYB("Query28bOptJoinTreeOptOrderingYB"),
    Query28cOptJoinTreeOptOrderingYB("Query28cOptJoinTreeOptOrderingYB"),
    Query29aOptJoinTreeOptOrderingYB("Query29aOptJoinTreeOptOrderingYB"),
    Query29bOptJoinTreeOptOrderingYB("Query29bOptJoinTreeOptOrderingYB"),
    Query29cOptJoinTreeOptOrderingYB("Query29cOptJoinTreeOptOrderingYB"),
    Query2aOptJoinTreeOptOrderingYB("Query2aOptJoinTreeOptOrderingYB"),
    Query2bOptJoinTreeOptOrderingYB("Query2bOptJoinTreeOptOrderingYB"),
    Query2cOptJoinTreeOptOrderingYB("Query2cOptJoinTreeOptOrderingYB"),
    Query2dOptJoinTreeOptOrderingYB("Query2dOptJoinTreeOptOrderingYB"),
    Query30aOptJoinTreeOptOrderingYB("Query30aOptJoinTreeOptOrderingYB"),
    Query30bOptJoinTreeOptOrderingYB("Query30bOptJoinTreeOptOrderingYB"),
    Query30cOptJoinTreeOptOrderingYB("Query30cOptJoinTreeOptOrderingYB"),
    Query31aOptJoinTreeOptOrderingYB("Query31aOptJoinTreeOptOrderingYB"),
    Query31bOptJoinTreeOptOrderingYB("Query31bOptJoinTreeOptOrderingYB"),
    Query31cOptJoinTreeOptOrderingYB("Query31cOptJoinTreeOptOrderingYB"),
    Query32aOptJoinTreeOptOrderingYB("Query32aOptJoinTreeOptOrderingYB"),
    Query32bOptJoinTreeOptOrderingYB("Query32bOptJoinTreeOptOrderingYB"),
    Query33aOptJoinTreeOptOrderingYB("Query33aOptJoinTreeOptOrderingYB"),
    Query33bOptJoinTreeOptOrderingYB("Query33bOptJoinTreeOptOrderingYB"),
    Query33cOptJoinTreeOptOrderingYB("Query33cOptJoinTreeOptOrderingYB"),
    Query3aOptJoinTreeOptOrderingYB("Query3aOptJoinTreeOptOrderingYB"),
    Query3bOptJoinTreeOptOrderingYB("Query3bOptJoinTreeOptOrderingYB"),
    Query3cOptJoinTreeOptOrderingYB("Query3cOptJoinTreeOptOrderingYB"),
    Query4aOptJoinTreeOptOrderingYB("Query4aOptJoinTreeOptOrderingYB"),
    Query4bOptJoinTreeOptOrderingYB("Query4bOptJoinTreeOptOrderingYB"),
    Query4cOptJoinTreeOptOrderingYB("Query4cOptJoinTreeOptOrderingYB"),
    Query5aOptJoinTreeOptOrderingYB("Query5aOptJoinTreeOptOrderingYB"),
    Query5bOptJoinTreeOptOrderingYB("Query5bOptJoinTreeOptOrderingYB"),
    Query5cOptJoinTreeOptOrderingYB("Query5cOptJoinTreeOptOrderingYB"),
    Query6aOptJoinTreeOptOrderingYB("Query6aOptJoinTreeOptOrderingYB"),
    Query6bOptJoinTreeOptOrderingYB("Query6bOptJoinTreeOptOrderingYB"),
    Query6cOptJoinTreeOptOrderingYB("Query6cOptJoinTreeOptOrderingYB"),
    Query6dOptJoinTreeOptOrderingYB("Query6dOptJoinTreeOptOrderingYB"),
    Query6eOptJoinTreeOptOrderingYB("Query6eOptJoinTreeOptOrderingYB"),
    Query6fOptJoinTreeOptOrderingYB("Query6fOptJoinTreeOptOrderingYB"),
    Query7aOptJoinTreeOptOrderingYB("Query7aOptJoinTreeOptOrderingYB"),
    Query7bOptJoinTreeOptOrderingYB("Query7bOptJoinTreeOptOrderingYB"),
    Query7cOptJoinTreeOptOrderingYB("Query7cOptJoinTreeOptOrderingYB"),
    Query8aOptJoinTreeOptOrderingYB("Query8aOptJoinTreeOptOrderingYB"),
    Query8bOptJoinTreeOptOrderingYB("Query8bOptJoinTreeOptOrderingYB"),
    Query8cOptJoinTreeOptOrderingYB("Query8cOptJoinTreeOptOrderingYB"),
    Query8dOptJoinTreeOptOrderingYB("Query8dOptJoinTreeOptOrderingYB"),
    Query9aOptJoinTreeOptOrderingYB("Query9aOptJoinTreeOptOrderingYB"),
    Query9bOptJoinTreeOptOrderingYB("Query9bOptJoinTreeOptOrderingYB"),
    Query9cOptJoinTreeOptOrderingYB("Query9cOptJoinTreeOptOrderingYB"),
    Query9dOptJoinTreeOptOrderingYB("Query9dOptJoinTreeOptOrderingYB");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (JOBQueriesYannakakisB c : values()) {
            switch (c) {
                case Query10aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query10aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query10bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query10bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query10cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query10cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query11aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query11aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query11bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query11bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query11cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query11cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query11dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query11dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query12aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query12aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query12bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query12bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query12cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query12cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query13aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query13aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query13bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query13bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query13cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query13cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query13dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query13dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query14aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query14aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query14bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query14bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query14cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query14cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query15aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query15aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query15bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query15bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query15cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query15cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query15dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query15dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query16aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query16aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query16bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query16bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query16cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query16cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query16dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query16dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query17aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query17aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query17bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query17bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query17cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query17cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query17dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query17dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query17eOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query17eOptJoinTreeOptOrderingYB.class);
                    break;
                case Query17fOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query17fOptJoinTreeOptOrderingYB.class);
                    break;
                case Query18aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query18aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query18bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query18bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query18cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query18cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query19aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query19aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query19bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query19bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query19cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query19cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query19dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query19dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query1aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query1aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query1bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query1bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query1cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query1cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query1dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query1dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query20aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query20aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query20bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query20bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query20cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query20cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query21aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query21aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query21bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query21bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query21cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query21cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query22aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query22aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query22bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query22bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query22cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query22cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query22dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query22dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query23aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query23aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query23bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query23bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query23cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query23cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query24aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query24aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query24bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query24bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query25aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query25aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query25bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query25bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query25cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query25cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query26aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query26aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query26bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query26bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query26cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query26cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query27aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query27aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query27bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query27bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query27cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query27cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query28aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query28aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query28bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query28bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query28cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query28cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query29aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query29aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query29bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query29bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query29cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query29cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query2aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query2aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query2bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query2bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query2cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query2cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query2dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query2dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query30aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query30aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query30bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query30bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query30cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query30cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query31aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query31aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query31bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query31bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query31cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query31cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query32aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query32aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query32bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query32bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query33aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query33aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query33bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query33bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query33cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query33cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query3aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query3aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query3bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query3bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query3cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query3cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query4aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query4aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query4bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query4bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query4cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query4cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query5aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query5aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query5bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query5bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query5cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query5cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query6aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query6aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query6bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query6bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query6cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query6cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query6dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query6dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query6eOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query6eOptJoinTreeOptOrderingYB.class);
                    break;
                case Query6fOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query6fOptJoinTreeOptOrderingYB.class);
                    break;
                case Query7aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query7aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query7bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query7bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query7cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query7cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query8aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query8aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query8bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query8bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query8cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query8cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query8dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query8dOptJoinTreeOptOrderingYB.class);
                    break;
                case Query9aOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query9aOptJoinTreeOptOrderingYB.class);
                    break;
                case Query9bOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query9bOptJoinTreeOptOrderingYB.class);
                    break;
                case Query9cOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query9cOptJoinTreeOptOrderingYB.class);
                    break;
                case Query9dOptJoinTreeOptOrderingYB:
                    map.put(c.val, Query9dOptJoinTreeOptOrderingYB.class);
                    break;
            }
        }
    }

    private final String val;

    private JOBQueriesYannakakisB(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
