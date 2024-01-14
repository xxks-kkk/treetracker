package org.zhu45.treetracker.benchmark;

import org.apache.commons.lang3.tuple.Pair;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.benchmark.QueryProvider.benchmarkQuery;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

/***
 * This benchmark demonstrates the performance gain of keeping jav instead of tuples in ng
 */
@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(1)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class JavEnhancementBenchmark
{
    @Benchmark
    public void TTJ()
    {
        benchmarkQuery(JoinOperator.TTJ, JavEnhancementQuery.class);
    }

    @Benchmark
    public void TTJHP()
    {
        benchmarkQuery(JoinOperator.TTJHP, JavEnhancementQuery.class);
    }

    public static class JavEnhancementQuery
            extends Query
    {
        public JavEnhancementQuery(JoinFragmentContext context)
        {
            super(context);
        }

        @Override
        protected Pair<Plan, List<Operator>> constructQuery()
        {
            MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.NOPREDICATE);
            MultiwayJoinNode castInfNode = getCastInfoInt(JOBQueries.NOPREDICATE);
            MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.NOPREDICATE, null);

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                    asEdge(castInfNode, akaNameNode),
                    asEdge(castInfNode, movieCompaniesNode)), castInfNode);

            Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
            Plan plan = pair.getKey();

            LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                    Arrays.asList(castInfNode.getSchemaTableName(),
                            akaNameNode.getSchemaTableName(),
                            movieCompaniesNode.getSchemaTableName()));
            assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
            return pair;
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(JavEnhancementBenchmark.class).run();
    }
}
