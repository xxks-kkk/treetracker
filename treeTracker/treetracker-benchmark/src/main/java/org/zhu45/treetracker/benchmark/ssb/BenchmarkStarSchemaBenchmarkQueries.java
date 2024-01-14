package org.zhu45.treetracker.benchmark.ssb;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLIPHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedLIPTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.rule.AttachFullReducer;

import java.util.List;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Thread;
import static org.zhu45.treetracker.benchmark.Benchmarks.benchmark;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@State(Thread)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkStarSchemaBenchmarkQueries
{
    private JoinFragmentContext ttjContext;
    private JoinFragmentContext hashJoinContext;
    private JoinFragmentContext lipContext;
    private JoinFragmentContext yannakakisContext;

    @Setup
    public void setUp()
    {
        ttjContext = JoinFragmentContext.builder()
                .setIsDevMode(false)
                .build();
        hashJoinContext = JoinFragmentContext.builder()
                .setIsDevMode(false)
                .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class)))
                .build();
        lipContext = JoinFragmentContext.builder()
                .setIsDevMode(false)
                .setOperatorMap(createMap(Optional.of(TupleBasedLIPTableScanOperator.class), Optional.of(TupleBasedLIPHashJoinOperator.class)))
                .build();
        yannakakisContext = JoinFragmentContext.builder()
                .setIsDevMode(false)
                .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class)))
                .setRules(List.of(new AttachFullReducer()))
                .build();
    }

    @Benchmark
    public void queryOneTTJ()
    {
        QueryOne queryOneTTJ = new QueryOne(ttjContext);
        try {
            queryOneTTJ.eval();
        }
        finally {
            queryOneTTJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryOneHashJoin()
    {
        QueryOne queryOneHJ = new QueryOne(hashJoinContext);
        try {
            queryOneHJ.eval();
        }
        finally {
            queryOneHJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryOneLIP()
    {
        QueryOne queryOneLIP = new QueryOne(lipContext);
        try {
            queryOneLIP.eval();
        }
        finally {
            queryOneLIP.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryOneYannakakis()
    {
        QueryOne queryOneYannakakis = new QueryOne(yannakakisContext);
        try {
            queryOneYannakakis.eval();
        }
        finally {
            queryOneYannakakis.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryTwoTTJ()
    {
        QueryTwo queryTwoTTJ = new QueryTwo(ttjContext);
        try {
            queryTwoTTJ.eval();
        }
        finally {
            queryTwoTTJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryTwoHashJoin()
    {
        QueryTwo queryTwoHJ = new QueryTwo(hashJoinContext);
        try {
            queryTwoHJ.eval();
        }
        finally {
            queryTwoHJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryTwoLIP()
    {
        QueryTwo queryTwoLIP = new QueryTwo(lipContext);
        try {
            queryTwoLIP.eval();
        }
        finally {
            queryTwoLIP.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryTwoYannakakis()
    {
        QueryTwo queryTwoYannakakis = new QueryTwo(yannakakisContext);
        try {
            queryTwoYannakakis.eval();
        }
        finally {
            queryTwoYannakakis.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryThreeTTJ()
    {
        QueryThree queryThreeTTJ = new QueryThree(ttjContext);
        try {
            queryThreeTTJ.eval();
        }
        finally {
            queryThreeTTJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryThreeHashJoin()
    {
        QueryThree queryThreeHJ = new QueryThree(hashJoinContext);
        try {
            queryThreeHJ.eval();
        }
        finally {
            queryThreeHJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryThreeLIP()
    {
        QueryThree queryThreeLIP = new QueryThree(lipContext);
        try {
            queryThreeLIP.eval();
        }
        finally {
            queryThreeLIP.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryThreeYannakakis()
    {
        QueryThree queryThreeYannakakis = new QueryThree(yannakakisContext);
        try {
            queryThreeYannakakis.eval();
        }
        finally {
            queryThreeYannakakis.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryFourTTJ()
    {
        QueryFour queryFourTTJ = new QueryFour(ttjContext);
        try {
            queryFourTTJ.eval();
        }
        finally {
            queryFourTTJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryFourHashJoin()
    {
        QueryFour queryFourHJ = new QueryFour(hashJoinContext);
        try {
            queryFourHJ.eval();
        }
        finally {
            queryFourHJ.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryFourLIP()
    {
        QueryFour queryFourLIP = new QueryFour(lipContext);
        try {
            queryFourLIP.eval();
        }
        finally {
            queryFourLIP.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @Benchmark
    public void queryFourYannakakis()
    {
        QueryFour queryFourYannakakis = new QueryFour(yannakakisContext);
        try {
            queryFourYannakakis.eval();
        }
        finally {
            queryFourYannakakis.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkStarSchemaBenchmarkQueries.class).run();
    }
}
