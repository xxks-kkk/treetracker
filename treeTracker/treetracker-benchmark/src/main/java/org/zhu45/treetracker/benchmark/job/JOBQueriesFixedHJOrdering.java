package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q1.Query1aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17eOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17fOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q24.Query24aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q24.Query24bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q32.Query32aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q32.Query32bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6eOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6fOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8dOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9aOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9bOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9cOptJoinTreeOptOrderingFixedHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9dOptJoinTreeOptOrderingFixedHJOrdering;

import java.util.HashMap;
import java.util.Map;

/**
 * Generated by GenerateJOBBenchmarkWithOptimalJoinTrees
 */
public enum JOBQueriesFixedHJOrdering
        implements QueryEnum
{
    Query10aOptJoinTreeOptOrderingFixedHJOrdering("Query10aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query10bOptJoinTreeOptOrderingFixedHJOrdering("Query10bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query10cOptJoinTreeOptOrderingFixedHJOrdering("Query10cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query11aOptJoinTreeOptOrderingFixedHJOrdering("Query11aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query11bOptJoinTreeOptOrderingFixedHJOrdering("Query11bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query11cOptJoinTreeOptOrderingFixedHJOrdering("Query11cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query11dOptJoinTreeOptOrderingFixedHJOrdering("Query11dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query12aOptJoinTreeOptOrderingFixedHJOrdering("Query12aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query12bOptJoinTreeOptOrderingFixedHJOrdering("Query12bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query12cOptJoinTreeOptOrderingFixedHJOrdering("Query12cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query13aOptJoinTreeOptOrderingFixedHJOrdering("Query13aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query13bOptJoinTreeOptOrderingFixedHJOrdering("Query13bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query13cOptJoinTreeOptOrderingFixedHJOrdering("Query13cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query13dOptJoinTreeOptOrderingFixedHJOrdering("Query13dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query14aOptJoinTreeOptOrderingFixedHJOrdering("Query14aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query14bOptJoinTreeOptOrderingFixedHJOrdering("Query14bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query14cOptJoinTreeOptOrderingFixedHJOrdering("Query14cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query15aOptJoinTreeOptOrderingFixedHJOrdering("Query15aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query15bOptJoinTreeOptOrderingFixedHJOrdering("Query15bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query15cOptJoinTreeOptOrderingFixedHJOrdering("Query15cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query15dOptJoinTreeOptOrderingFixedHJOrdering("Query15dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query16aOptJoinTreeOptOrderingFixedHJOrdering("Query16aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query16bOptJoinTreeOptOrderingFixedHJOrdering("Query16bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query16cOptJoinTreeOptOrderingFixedHJOrdering("Query16cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query16dOptJoinTreeOptOrderingFixedHJOrdering("Query16dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query17aOptJoinTreeOptOrderingFixedHJOrdering("Query17aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query17bOptJoinTreeOptOrderingFixedHJOrdering("Query17bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query17cOptJoinTreeOptOrderingFixedHJOrdering("Query17cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query17dOptJoinTreeOptOrderingFixedHJOrdering("Query17dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query17eOptJoinTreeOptOrderingFixedHJOrdering("Query17eOptJoinTreeOptOrderingFixedHJOrdering"),
    Query17fOptJoinTreeOptOrderingFixedHJOrdering("Query17fOptJoinTreeOptOrderingFixedHJOrdering"),
    Query18aOptJoinTreeOptOrderingFixedHJOrdering("Query18aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query18bOptJoinTreeOptOrderingFixedHJOrdering("Query18bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query18cOptJoinTreeOptOrderingFixedHJOrdering("Query18cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query19aOptJoinTreeOptOrderingFixedHJOrdering("Query19aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query19bOptJoinTreeOptOrderingFixedHJOrdering("Query19bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query19cOptJoinTreeOptOrderingFixedHJOrdering("Query19cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query19dOptJoinTreeOptOrderingFixedHJOrdering("Query19dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query1aOptJoinTreeOptOrderingFixedHJOrdering("Query1aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query1bOptJoinTreeOptOrderingFixedHJOrdering("Query1bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query1cOptJoinTreeOptOrderingFixedHJOrdering("Query1cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query1dOptJoinTreeOptOrderingFixedHJOrdering("Query1dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query20aOptJoinTreeOptOrderingFixedHJOrdering("Query20aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query20bOptJoinTreeOptOrderingFixedHJOrdering("Query20bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query20cOptJoinTreeOptOrderingFixedHJOrdering("Query20cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query21aOptJoinTreeOptOrderingFixedHJOrdering("Query21aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query21bOptJoinTreeOptOrderingFixedHJOrdering("Query21bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query21cOptJoinTreeOptOrderingFixedHJOrdering("Query21cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query22aOptJoinTreeOptOrderingFixedHJOrdering("Query22aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query22bOptJoinTreeOptOrderingFixedHJOrdering("Query22bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query22cOptJoinTreeOptOrderingFixedHJOrdering("Query22cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query22dOptJoinTreeOptOrderingFixedHJOrdering("Query22dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query23aOptJoinTreeOptOrderingFixedHJOrdering("Query23aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query23bOptJoinTreeOptOrderingFixedHJOrdering("Query23bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query23cOptJoinTreeOptOrderingFixedHJOrdering("Query23cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query24aOptJoinTreeOptOrderingFixedHJOrdering("Query24aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query24bOptJoinTreeOptOrderingFixedHJOrdering("Query24bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query25aOptJoinTreeOptOrderingFixedHJOrdering("Query25aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query25bOptJoinTreeOptOrderingFixedHJOrdering("Query25bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query25cOptJoinTreeOptOrderingFixedHJOrdering("Query25cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query26aOptJoinTreeOptOrderingFixedHJOrdering("Query26aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query26bOptJoinTreeOptOrderingFixedHJOrdering("Query26bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query26cOptJoinTreeOptOrderingFixedHJOrdering("Query26cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query27aOptJoinTreeOptOrderingFixedHJOrdering("Query27aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query27bOptJoinTreeOptOrderingFixedHJOrdering("Query27bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query27cOptJoinTreeOptOrderingFixedHJOrdering("Query27cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query28aOptJoinTreeOptOrderingFixedHJOrdering("Query28aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query28bOptJoinTreeOptOrderingFixedHJOrdering("Query28bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query28cOptJoinTreeOptOrderingFixedHJOrdering("Query28cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query29aOptJoinTreeOptOrderingFixedHJOrdering("Query29aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query29bOptJoinTreeOptOrderingFixedHJOrdering("Query29bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query29cOptJoinTreeOptOrderingFixedHJOrdering("Query29cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query2aOptJoinTreeOptOrderingFixedHJOrdering("Query2aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query2bOptJoinTreeOptOrderingFixedHJOrdering("Query2bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query2cOptJoinTreeOptOrderingFixedHJOrdering("Query2cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query2dOptJoinTreeOptOrderingFixedHJOrdering("Query2dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query30aOptJoinTreeOptOrderingFixedHJOrdering("Query30aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query30bOptJoinTreeOptOrderingFixedHJOrdering("Query30bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query30cOptJoinTreeOptOrderingFixedHJOrdering("Query30cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query31aOptJoinTreeOptOrderingFixedHJOrdering("Query31aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query31bOptJoinTreeOptOrderingFixedHJOrdering("Query31bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query31cOptJoinTreeOptOrderingFixedHJOrdering("Query31cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query32aOptJoinTreeOptOrderingFixedHJOrdering("Query32aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query32bOptJoinTreeOptOrderingFixedHJOrdering("Query32bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query33aOptJoinTreeOptOrderingFixedHJOrdering("Query33aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query33bOptJoinTreeOptOrderingFixedHJOrdering("Query33bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query33cOptJoinTreeOptOrderingFixedHJOrdering("Query33cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query3aOptJoinTreeOptOrderingFixedHJOrdering("Query3aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query3bOptJoinTreeOptOrderingFixedHJOrdering("Query3bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query3cOptJoinTreeOptOrderingFixedHJOrdering("Query3cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query4aOptJoinTreeOptOrderingFixedHJOrdering("Query4aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query4bOptJoinTreeOptOrderingFixedHJOrdering("Query4bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query4cOptJoinTreeOptOrderingFixedHJOrdering("Query4cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query5aOptJoinTreeOptOrderingFixedHJOrdering("Query5aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query5bOptJoinTreeOptOrderingFixedHJOrdering("Query5bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query5cOptJoinTreeOptOrderingFixedHJOrdering("Query5cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query6aOptJoinTreeOptOrderingFixedHJOrdering("Query6aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query6bOptJoinTreeOptOrderingFixedHJOrdering("Query6bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query6cOptJoinTreeOptOrderingFixedHJOrdering("Query6cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query6dOptJoinTreeOptOrderingFixedHJOrdering("Query6dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query6eOptJoinTreeOptOrderingFixedHJOrdering("Query6eOptJoinTreeOptOrderingFixedHJOrdering"),
    Query6fOptJoinTreeOptOrderingFixedHJOrdering("Query6fOptJoinTreeOptOrderingFixedHJOrdering"),
    Query7aOptJoinTreeOptOrderingFixedHJOrdering("Query7aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query7bOptJoinTreeOptOrderingFixedHJOrdering("Query7bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query7cOptJoinTreeOptOrderingFixedHJOrdering("Query7cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query8aOptJoinTreeOptOrderingFixedHJOrdering("Query8aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query8bOptJoinTreeOptOrderingFixedHJOrdering("Query8bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query8cOptJoinTreeOptOrderingFixedHJOrdering("Query8cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query8dOptJoinTreeOptOrderingFixedHJOrdering("Query8dOptJoinTreeOptOrderingFixedHJOrdering"),
    Query9aOptJoinTreeOptOrderingFixedHJOrdering("Query9aOptJoinTreeOptOrderingFixedHJOrdering"),
    Query9bOptJoinTreeOptOrderingFixedHJOrdering("Query9bOptJoinTreeOptOrderingFixedHJOrdering"),
    Query9cOptJoinTreeOptOrderingFixedHJOrdering("Query9cOptJoinTreeOptOrderingFixedHJOrdering"),
    Query9dOptJoinTreeOptOrderingFixedHJOrdering("Query9dOptJoinTreeOptOrderingFixedHJOrdering");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (JOBQueriesFixedHJOrdering c : values()) {
            switch (c) {
                case Query10aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query10aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query10bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query10bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query10cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query10cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query11aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query11aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query11bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query11bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query11cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query11cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query11dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query11dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query12aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query12aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query12bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query12bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query12cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query12cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query13aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query13aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query13bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query13bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query13cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query13cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query13dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query13dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query14aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query14aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query14bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query14bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query14cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query14cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query15aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query15aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query15bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query15bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query15cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query15cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query15dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query15dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query16aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query16aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query16bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query16bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query16cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query16cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query16dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query16dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query17aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query17aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query17bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query17bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query17cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query17cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query17dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query17dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query17eOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query17eOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query17fOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query17fOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query18aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query18aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query18bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query18bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query18cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query18cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query19aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query19aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query19bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query19bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query19cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query19cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query19dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query19dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query1aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query1aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query1bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query1bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query1cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query1cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query1dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query1dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query20aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query20aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query20bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query20bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query20cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query20cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query21aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query21aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query21bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query21bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query21cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query21cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query22aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query22aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query22bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query22bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query22cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query22cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query22dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query22dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query23aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query23aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query23bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query23bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query23cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query23cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query24aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query24aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query24bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query24bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query25aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query25aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query25bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query25bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query25cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query25cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query26aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query26aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query26bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query26bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query26cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query26cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query27aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query27aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query27bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query27bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query27cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query27cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query28aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query28aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query28bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query28bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query28cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query28cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query29aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query29aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query29bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query29bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query29cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query29cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query2aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query2aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query2bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query2bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query2cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query2cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query2dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query2dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query30aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query30aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query30bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query30bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query30cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query30cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query31aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query31aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query31bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query31bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query31cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query31cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query32aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query32aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query32bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query32bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query33aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query33aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query33bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query33bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query33cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query33cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query3aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query3aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query3bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query3bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query3cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query3cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query4aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query4aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query4bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query4bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query4cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query4cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query5aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query5aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query5bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query5bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query5cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query5cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query6aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query6aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query6bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query6bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query6cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query6cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query6dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query6dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query6eOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query6eOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query6fOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query6fOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query7aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query7aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query7bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query7bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query7cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query7cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query8aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query8aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query8bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query8bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query8cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query8cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query8dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query8dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query9aOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query9aOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query9bOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query9bOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query9cOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query9cOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
                case Query9dOptJoinTreeOptOrderingFixedHJOrdering:
                    map.put(c.val, Query9dOptJoinTreeOptOrderingFixedHJOrdering.class);
                    break;
            }
        }
    }

    private final String val;

    private JOBQueriesFixedHJOrdering(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
