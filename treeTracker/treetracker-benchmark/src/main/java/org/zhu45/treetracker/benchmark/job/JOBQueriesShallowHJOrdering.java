package org.zhu45.treetracker.benchmark.job;

import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryEnum;
import org.zhu45.treetracker.benchmark.job.q1.Query1aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q1.Query1dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q10.Query10cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q11.Query11dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q12.Query12cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q13.Query13dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q14.Query14cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q15.Query15dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q16.Query16dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17eOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q17.Query17fOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q18.Query18cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q19.Query19dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q2.Query2dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q20.Query20cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q21.Query21cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q22.Query22dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q23.Query23cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q24.Query24aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q24.Query24bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q26.Query26cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q27.Query27cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q28.Query28cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q29.Query29cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q3.Query3cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q30.Query30cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q31.Query31cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q32.Query32aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q32.Query32bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q33.Query33cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q4.Query4cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q5.Query5cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6eOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q6.Query6fOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q7.Query7cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q8.Query8dOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9aOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9bOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9cOptJoinTreeOptOrderingShallowHJOrdering;
import org.zhu45.treetracker.benchmark.job.q9.Query9dOptJoinTreeOptOrderingShallowHJOrdering;

import java.util.HashMap;
import java.util.Map;

/**
 * Generated by GenerateJOBBenchmarkWithOptimalJoinTrees
 */
public enum JOBQueriesShallowHJOrdering
        implements QueryEnum
{
    Query10aOptJoinTreeOptOrderingShallowHJOrdering("Query10aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query10bOptJoinTreeOptOrderingShallowHJOrdering("Query10bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query10cOptJoinTreeOptOrderingShallowHJOrdering("Query10cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query11aOptJoinTreeOptOrderingShallowHJOrdering("Query11aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query11bOptJoinTreeOptOrderingShallowHJOrdering("Query11bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query11cOptJoinTreeOptOrderingShallowHJOrdering("Query11cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query11dOptJoinTreeOptOrderingShallowHJOrdering("Query11dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query12aOptJoinTreeOptOrderingShallowHJOrdering("Query12aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query12bOptJoinTreeOptOrderingShallowHJOrdering("Query12bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query12cOptJoinTreeOptOrderingShallowHJOrdering("Query12cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query13aOptJoinTreeOptOrderingShallowHJOrdering("Query13aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query13bOptJoinTreeOptOrderingShallowHJOrdering("Query13bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query13cOptJoinTreeOptOrderingShallowHJOrdering("Query13cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query13dOptJoinTreeOptOrderingShallowHJOrdering("Query13dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query14aOptJoinTreeOptOrderingShallowHJOrdering("Query14aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query14bOptJoinTreeOptOrderingShallowHJOrdering("Query14bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query14cOptJoinTreeOptOrderingShallowHJOrdering("Query14cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query15aOptJoinTreeOptOrderingShallowHJOrdering("Query15aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query15bOptJoinTreeOptOrderingShallowHJOrdering("Query15bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query15cOptJoinTreeOptOrderingShallowHJOrdering("Query15cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query15dOptJoinTreeOptOrderingShallowHJOrdering("Query15dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query16aOptJoinTreeOptOrderingShallowHJOrdering("Query16aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query16bOptJoinTreeOptOrderingShallowHJOrdering("Query16bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query16cOptJoinTreeOptOrderingShallowHJOrdering("Query16cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query16dOptJoinTreeOptOrderingShallowHJOrdering("Query16dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query17aOptJoinTreeOptOrderingShallowHJOrdering("Query17aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query17bOptJoinTreeOptOrderingShallowHJOrdering("Query17bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query17cOptJoinTreeOptOrderingShallowHJOrdering("Query17cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query17dOptJoinTreeOptOrderingShallowHJOrdering("Query17dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query17eOptJoinTreeOptOrderingShallowHJOrdering("Query17eOptJoinTreeOptOrderingShallowHJOrdering"),
    Query17fOptJoinTreeOptOrderingShallowHJOrdering("Query17fOptJoinTreeOptOrderingShallowHJOrdering"),
    Query18aOptJoinTreeOptOrderingShallowHJOrdering("Query18aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query18bOptJoinTreeOptOrderingShallowHJOrdering("Query18bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query18cOptJoinTreeOptOrderingShallowHJOrdering("Query18cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query19aOptJoinTreeOptOrderingShallowHJOrdering("Query19aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query19bOptJoinTreeOptOrderingShallowHJOrdering("Query19bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query19cOptJoinTreeOptOrderingShallowHJOrdering("Query19cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query19dOptJoinTreeOptOrderingShallowHJOrdering("Query19dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query1aOptJoinTreeOptOrderingShallowHJOrdering("Query1aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query1bOptJoinTreeOptOrderingShallowHJOrdering("Query1bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query1cOptJoinTreeOptOrderingShallowHJOrdering("Query1cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query1dOptJoinTreeOptOrderingShallowHJOrdering("Query1dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query20aOptJoinTreeOptOrderingShallowHJOrdering("Query20aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query20bOptJoinTreeOptOrderingShallowHJOrdering("Query20bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query20cOptJoinTreeOptOrderingShallowHJOrdering("Query20cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query21aOptJoinTreeOptOrderingShallowHJOrdering("Query21aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query21bOptJoinTreeOptOrderingShallowHJOrdering("Query21bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query21cOptJoinTreeOptOrderingShallowHJOrdering("Query21cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query22aOptJoinTreeOptOrderingShallowHJOrdering("Query22aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query22bOptJoinTreeOptOrderingShallowHJOrdering("Query22bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query22cOptJoinTreeOptOrderingShallowHJOrdering("Query22cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query22dOptJoinTreeOptOrderingShallowHJOrdering("Query22dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query23aOptJoinTreeOptOrderingShallowHJOrdering("Query23aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query23bOptJoinTreeOptOrderingShallowHJOrdering("Query23bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query23cOptJoinTreeOptOrderingShallowHJOrdering("Query23cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query24aOptJoinTreeOptOrderingShallowHJOrdering("Query24aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query24bOptJoinTreeOptOrderingShallowHJOrdering("Query24bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query25aOptJoinTreeOptOrderingShallowHJOrdering("Query25aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query25bOptJoinTreeOptOrderingShallowHJOrdering("Query25bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query25cOptJoinTreeOptOrderingShallowHJOrdering("Query25cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query26aOptJoinTreeOptOrderingShallowHJOrdering("Query26aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query26bOptJoinTreeOptOrderingShallowHJOrdering("Query26bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query26cOptJoinTreeOptOrderingShallowHJOrdering("Query26cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query27aOptJoinTreeOptOrderingShallowHJOrdering("Query27aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query27bOptJoinTreeOptOrderingShallowHJOrdering("Query27bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query27cOptJoinTreeOptOrderingShallowHJOrdering("Query27cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query28aOptJoinTreeOptOrderingShallowHJOrdering("Query28aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query28bOptJoinTreeOptOrderingShallowHJOrdering("Query28bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query28cOptJoinTreeOptOrderingShallowHJOrdering("Query28cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query29aOptJoinTreeOptOrderingShallowHJOrdering("Query29aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query29bOptJoinTreeOptOrderingShallowHJOrdering("Query29bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query29cOptJoinTreeOptOrderingShallowHJOrdering("Query29cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query2aOptJoinTreeOptOrderingShallowHJOrdering("Query2aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query2bOptJoinTreeOptOrderingShallowHJOrdering("Query2bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query2cOptJoinTreeOptOrderingShallowHJOrdering("Query2cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query2dOptJoinTreeOptOrderingShallowHJOrdering("Query2dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query30aOptJoinTreeOptOrderingShallowHJOrdering("Query30aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query30bOptJoinTreeOptOrderingShallowHJOrdering("Query30bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query30cOptJoinTreeOptOrderingShallowHJOrdering("Query30cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query31aOptJoinTreeOptOrderingShallowHJOrdering("Query31aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query31bOptJoinTreeOptOrderingShallowHJOrdering("Query31bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query31cOptJoinTreeOptOrderingShallowHJOrdering("Query31cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query32aOptJoinTreeOptOrderingShallowHJOrdering("Query32aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query32bOptJoinTreeOptOrderingShallowHJOrdering("Query32bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query33aOptJoinTreeOptOrderingShallowHJOrdering("Query33aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query33bOptJoinTreeOptOrderingShallowHJOrdering("Query33bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query33cOptJoinTreeOptOrderingShallowHJOrdering("Query33cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query3aOptJoinTreeOptOrderingShallowHJOrdering("Query3aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query3bOptJoinTreeOptOrderingShallowHJOrdering("Query3bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query3cOptJoinTreeOptOrderingShallowHJOrdering("Query3cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query4aOptJoinTreeOptOrderingShallowHJOrdering("Query4aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query4bOptJoinTreeOptOrderingShallowHJOrdering("Query4bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query4cOptJoinTreeOptOrderingShallowHJOrdering("Query4cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query5aOptJoinTreeOptOrderingShallowHJOrdering("Query5aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query5bOptJoinTreeOptOrderingShallowHJOrdering("Query5bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query5cOptJoinTreeOptOrderingShallowHJOrdering("Query5cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query6aOptJoinTreeOptOrderingShallowHJOrdering("Query6aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query6bOptJoinTreeOptOrderingShallowHJOrdering("Query6bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query6cOptJoinTreeOptOrderingShallowHJOrdering("Query6cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query6dOptJoinTreeOptOrderingShallowHJOrdering("Query6dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query6eOptJoinTreeOptOrderingShallowHJOrdering("Query6eOptJoinTreeOptOrderingShallowHJOrdering"),
    Query6fOptJoinTreeOptOrderingShallowHJOrdering("Query6fOptJoinTreeOptOrderingShallowHJOrdering"),
    Query7aOptJoinTreeOptOrderingShallowHJOrdering("Query7aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query7bOptJoinTreeOptOrderingShallowHJOrdering("Query7bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query7cOptJoinTreeOptOrderingShallowHJOrdering("Query7cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query8aOptJoinTreeOptOrderingShallowHJOrdering("Query8aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query8bOptJoinTreeOptOrderingShallowHJOrdering("Query8bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query8cOptJoinTreeOptOrderingShallowHJOrdering("Query8cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query8dOptJoinTreeOptOrderingShallowHJOrdering("Query8dOptJoinTreeOptOrderingShallowHJOrdering"),
    Query9aOptJoinTreeOptOrderingShallowHJOrdering("Query9aOptJoinTreeOptOrderingShallowHJOrdering"),
    Query9bOptJoinTreeOptOrderingShallowHJOrdering("Query9bOptJoinTreeOptOrderingShallowHJOrdering"),
    Query9cOptJoinTreeOptOrderingShallowHJOrdering("Query9cOptJoinTreeOptOrderingShallowHJOrdering"),
    Query9dOptJoinTreeOptOrderingShallowHJOrdering("Query9dOptJoinTreeOptOrderingShallowHJOrdering");

    private static final Map<String, Class<? extends Query>> map = new HashMap<>(values().length, 1);

    static {
        for (JOBQueriesShallowHJOrdering c : values()) {
            switch (c) {
                case Query10aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query10aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query10bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query10bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query10cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query10cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query11aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query11aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query11bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query11bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query11cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query11cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query11dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query11dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query12aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query12aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query12bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query12bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query12cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query12cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query13aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query13aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query13bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query13bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query13cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query13cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query13dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query13dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query14aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query14aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query14bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query14bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query14cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query14cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query15aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query15aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query15bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query15bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query15cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query15cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query15dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query15dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query16aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query16aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query16bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query16bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query16cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query16cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query16dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query16dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query17aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query17aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query17bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query17bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query17cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query17cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query17dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query17dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query17eOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query17eOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query17fOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query17fOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query18aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query18aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query18bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query18bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query18cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query18cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query19aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query19aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query19bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query19bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query19cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query19cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query19dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query19dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query1aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query1aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query1bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query1bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query1cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query1cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query1dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query1dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query20aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query20aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query20bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query20bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query20cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query20cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query21aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query21aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query21bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query21bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query21cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query21cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query22aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query22aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query22bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query22bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query22cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query22cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query22dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query22dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query23aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query23aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query23bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query23bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query23cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query23cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query24aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query24aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query24bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query24bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query25aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query25aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query25bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query25bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query25cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query25cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query26aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query26aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query26bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query26bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query26cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query26cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query27aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query27aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query27bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query27bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query27cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query27cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query28aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query28aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query28bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query28bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query28cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query28cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query29aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query29aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query29bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query29bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query29cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query29cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query2aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query2aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query2bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query2bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query2cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query2cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query2dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query2dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query30aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query30aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query30bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query30bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query30cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query30cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query31aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query31aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query31bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query31bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query31cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query31cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query32aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query32aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query32bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query32bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query33aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query33aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query33bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query33bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query33cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query33cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query3aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query3aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query3bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query3bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query3cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query3cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query4aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query4aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query4bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query4bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query4cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query4cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query5aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query5aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query5bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query5bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query5cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query5cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query6aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query6aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query6bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query6bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query6cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query6cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query6dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query6dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query6eOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query6eOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query6fOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query6fOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query7aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query7aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query7bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query7bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query7cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query7cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query8aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query8aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query8bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query8bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query8cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query8cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query8dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query8dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query9aOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query9aOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query9bOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query9bOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query9cOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query9cOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
                case Query9dOptJoinTreeOptOrderingShallowHJOrdering:
                    map.put(c.val, Query9dOptJoinTreeOptOrderingShallowHJOrdering.class);
                    break;
            }
        }
    }

    private final String val;

    private JOBQueriesShallowHJOrdering(String s)
    {
        val = s;
    }

    @Override
    public Class<? extends Query> getQueryClazz()
    {
        return map.get(val);
    }
}
