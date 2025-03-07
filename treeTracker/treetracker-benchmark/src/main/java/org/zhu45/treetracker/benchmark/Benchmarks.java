package org.zhu45.treetracker.benchmark;

import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.function.Consumer;

import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;

public final class Benchmarks
{
    private Benchmarks()
    {
    }

    public static final String ROOT;
    public static final String JOB_RESULT_STORED_PATH;
    public static final String SSB_RESULT_STORED_PATH;
    public static final String SSB_SQLITE_ORDERING_STORED_PATH;
    public static final String SSB_SQLITE_ORDERING_STATS_STORED_PATH;
    public static final String SSB_POSTGRES_PLAN_STORED_PATH;
    public static final String SSB_UPDATED_POSTGRES_PLAN_STORED_PATH;
    public static final String SSB_POSTGRES_RESULT_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_POSTGRES_PLAN_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_POSTGRES_RESULT_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_UPDATED_POSTGRES_PLAN_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_SHALLOW_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_TRUE_CARD_STORED_PATH;
    public static final String JOB_WITH_PREDICATES_RESULT_EXP2P3_STORED_PATH;
    public static final String TPCH_RESULT_STORED_PATH;
    public static final String TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
    public static final String TPCH_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH;
    public static final String TPCH_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH;
    public static final String TPCH_POSTGRES_PLAN_STORED_PATH;
    public static final String TPCH_UPDATED_POSTGRES_PLAN_STORED_PATH;
    public static final String TPCH_WITH_PREDICATES_RESULT_POSTGRES_RESULT_STORED_PATH;
    public static final String OTHERS_RESULT_STORED_PATH;
    public static final String SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
    public static final String SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SAME_ORDERING_STORED_PATH;
    public static final String SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
    public static final String SSB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
    public static final String TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
    public static final String JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH;
    public static final String SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_TRUE_CARD_STORED_PATH;
    public static final String COST_MODEL_4_RESULT_STORED_PATH;
    public static final String COST_MODEL_4_WITH_PREDICATES_RESULT_STORED_PATH;
    public static final String BACKJUMP_OTHERS_RESULT_STORED_PATH;
    public static final String MICROBENCH_QUERIES_RESULT_STORED_PATH;
    public static final String EXP2P5_RESULT_STORED_PATH;
    public static final String EXP2P8_RESULT_STORED_PATH;
    public static final String EXP2P9_RESULT_STORED_PATH;
    public static final String EXP2P9O_RESULT_STORED_PATH;

    static {
        // NOTE: To run `TestCheckQueryImplementationCorrectness`, we need to set ROOT as `Paths.get(".").toAbsolutePath().getParent().getParent().getParent().toString();`
        ROOT = Paths.get(System.getProperty("user.dir")).getParent().toString();
        String resultPath = Paths.get(ROOT, "results").toString();
        JOB_RESULT_STORED_PATH = Paths.get(resultPath, "job").toString();
        SSB_RESULT_STORED_PATH = Paths.get(resultPath, "ssb").toString();
        SSB_SQLITE_ORDERING_STORED_PATH = Paths.get(SSB_RESULT_STORED_PATH, "hj_ordering_hj").toString();
        SSB_SQLITE_ORDERING_STATS_STORED_PATH = Paths.get(SSB_SQLITE_ORDERING_STORED_PATH, "stats").toString();
        SSB_POSTGRES_PLAN_STORED_PATH = Paths.get(SSB_RESULT_STORED_PATH, "postgres_plans").toString();
        SSB_UPDATED_POSTGRES_PLAN_STORED_PATH = Paths.get(SSB_RESULT_STORED_PATH, "postgres_plans_updated").toString();
        SSB_POSTGRES_RESULT_STORED_PATH = Paths.get(SSB_RESULT_STORED_PATH, "perf_on_postgres_plans").toString();
        JOB_WITH_PREDICATES_RESULT_STORED_PATH = Paths.get(resultPath, "job", "with_predicates").toString();
        JOB_WITH_PREDICATES_RESULT_POSTGRES_PLAN_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "postgres_plans").toString();
        JOB_WITH_PREDICATES_RESULT_POSTGRES_RESULT_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "perf_on_postgres_plans").toString();
        JOB_WITH_PREDICATES_RESULT_UPDATED_POSTGRES_PLAN_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "postgres_plans_updated").toString();
        JOB_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "hj_ordering_hj").toString();
        JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "hj_ordering_opt_jointree").toString();
        JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_SHALLOW_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "hj_ordering_shallow_opt_jointree").toString();
        JOB_WITH_PREDICATES_RESULT_TRUE_CARD_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "true_card").toString();
        JOB_WITH_PREDICATES_RESULT_EXP2P3_STORED_PATH = Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, "exp2.3").toString();
        TPCH_RESULT_STORED_PATH = Paths.get(resultPath, "tpch").toString();
        TPCH_WITH_PREDICATES_RESULT_STORED_PATH = Paths.get(resultPath, "tpch", "with_predicates").toString();
        TPCH_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH = Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, "hj_ordering_opt_jointree").toString();
        TPCH_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH = Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, "hj_ordering_hj").toString();
        TPCH_POSTGRES_PLAN_STORED_PATH = Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, "postgres_plans").toString();
        TPCH_UPDATED_POSTGRES_PLAN_STORED_PATH = Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, "postgres_plans_updated").toString();
        TPCH_WITH_PREDICATES_RESULT_POSTGRES_RESULT_STORED_PATH = Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, "perf_on_postgres_plans").toString();
        OTHERS_RESULT_STORED_PATH = Paths.get(resultPath, "others").toString();
        SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH = Paths.get(resultPath, "others", "simple-cost-model-with-predicates").toString();
        SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SAME_ORDERING_STORED_PATH = Paths.get(SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH, "hj_ordering_opt_jointree").toString();
        SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH = Paths.get(SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH, "hj_ordering_hj").toString();
        SSB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH = Paths.get(SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH, "ssb")
                .toString();
        TPCH_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH = Paths.get(SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH, "tpch")
                .toString();
        JOB_SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH = Paths.get(SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SQLITE_ORDERING_STORED_PATH, "job")
                .toString();
        SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_TRUE_CARD_STORED_PATH = Paths.get(SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH, "true_card").toString();
        COST_MODEL_4_RESULT_STORED_PATH = Paths.get(resultPath, "others", "cost-model4").toString();
        COST_MODEL_4_WITH_PREDICATES_RESULT_STORED_PATH = Paths.get(resultPath, "others", "cost-model4-with-predicates").toString();
        BACKJUMP_OTHERS_RESULT_STORED_PATH = Paths.get(resultPath, "others", "cost-model3", "backjump").toString();
        MICROBENCH_QUERIES_RESULT_STORED_PATH = Paths.get(resultPath, "others", "microbench").toString();
        EXP2P5_RESULT_STORED_PATH = Paths.get(OTHERS_RESULT_STORED_PATH, "exp2p5").toString();
        EXP2P8_RESULT_STORED_PATH = Paths.get(OTHERS_RESULT_STORED_PATH, "exp2p8").toString();
        EXP2P9_RESULT_STORED_PATH = Paths.get(OTHERS_RESULT_STORED_PATH, "exp2p9").toString();
        EXP2P9O_RESULT_STORED_PATH = Paths.get(OTHERS_RESULT_STORED_PATH, "exp2p9o").toString();
    }

    public static BenchmarkBuilder benchmark(Class<?> benchmarkClass)
    {
        ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include("^\\Q" + benchmarkClass.getName() + ".\\E")
                .resultFormat(ResultFormatType.JSON)
                .result(format("%s/%s-result-%s.json", System.getProperty("java.io.tmpdir"), benchmarkClass.getSimpleName(), ISO_DATE_TIME.format(LocalDateTime.now())));
        return new BenchmarkBuilder(optionsBuilder);
    }

    public static BenchmarkBuilder benchmark(Class<?> benchmarkClass, String resultStoredPath)
    {
        ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include("^\\Q" + benchmarkClass.getName() + ".\\E")
                .resultFormat(ResultFormatType.JSON)
                .result(format("%s/%s-result-%s.json", resultStoredPath, benchmarkClass.getSimpleName(), ISO_DATE_TIME.format(LocalDateTime.now())));
        return new BenchmarkBuilder(optionsBuilder);
    }

    public static BenchmarkBuilder benchmark(Class<?> benchmarkClass, WarmupMode warmupMode)
    {
        return benchmark(benchmarkClass)
                .withOptions(optionsBuilder -> optionsBuilder.warmupMode(warmupMode));
    }

    public static class BenchmarkBuilder
    {
        private final ChainedOptionsBuilder optionsBuilder;

        private BenchmarkBuilder(ChainedOptionsBuilder optionsBuilder)
        {
            this.optionsBuilder = requireNonNull(optionsBuilder, "optionsBuilder is null");
        }

        public BenchmarkBuilder withOptions(Consumer<ChainedOptionsBuilder> optionsConsumer)
        {
            optionsConsumer.accept(optionsBuilder);
            return this;
        }

        public Collection<RunResult> run()
                throws RunnerException
        {
            return new Runner(optionsBuilder.build()).run();
        }
    }
}
