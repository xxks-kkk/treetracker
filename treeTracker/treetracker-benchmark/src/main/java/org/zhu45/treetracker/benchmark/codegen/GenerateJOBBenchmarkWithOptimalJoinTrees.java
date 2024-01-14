package org.zhu45.treetracker.benchmark.codegen;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import lombok.Getter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zhu45.treetracker.benchmark.ssb.SSBDatabase;
import org.zhu45.treetracker.benchmark.tpch.TPCHDatabase;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.countMatches;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.codegen.GenerateJOBBenchmarkWithOptimalJoinTrees.Step1.step1Driver;
import static org.zhu45.treetracker.benchmark.codegen.GenerateJOBBenchmarkWithOptimalJoinTrees.Step2.step2Driver;
import static org.zhu45.treetracker.benchmark.codegen.GenerateJOBBenchmarkWithOptimalJoinTrees.Step3.step3Driver;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2SavePath;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2TempateYannakakis;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2Template;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.relation2MultiwayJoinNode;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.savePathPrefix;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.templatePathPrefix;

/**
 * Purpose:
 * <p>
 * Given a collection of optimal join trees, the program is able to generate class implementations for queries
 * that use the join trees found, e.g., Query1bOptJoinTreeOptOrdering.java and corresponding benchmark file to
 * drive those classes, e.g., BenchmarkOptJoinTreeOptOrderingJOBWithPredicates.java.
 * Then, the user is able to run the benchmark driver file and produce the final benchmark results for those queries.
 * </p>
 * Steps (Use JOB as an example):
 * 1. The program finds all the specified planStatistics.json and extract "optimalJoinTree" and "optimalJoinOrdering" fields
 * from it. Then, For each extracted optimal join tree and the ordering used to create the join tree,
 * the program creates *OptJoinTreeOptOrdering.java.
 * 2. After creating all the *OptJoinTreeOptOrdering.java, the program creates JOBQueriesAutoGen.java to register
 * those classes to be used in the benchmark.
 * 3. The program creates BenchmarkJOBWithPredicatesAutoGen.java to drive those queries. User can then run
 * BenchmarkJOBWithPredicatesAutoGen.java to execute the benchmark.
 * <p>
 * FIXME: even though the name is GenerateBenchmarkWithOptimalJoinTrees, it doesn't only work for JOB. We didn't
 * rename due to large effort in renaming templates
 */
public class GenerateJOBBenchmarkWithOptimalJoinTrees
{
    private static final String ttjHPJavaClassExtension = "OptJoinTreeOptOrdering";
    private static final String ttjHPFixedHJOrderingJavaClassExtension = "OptJoinTreeOptOrderingFixedHJOrdering";
    private static final String yannakakisJavaClassExtension = "OptJoinTreeOptOrderingY";
    private static final String yannakakisBJavaClassExtension = "OptJoinTreeOptOrderingYB";
    private static final String yannakakisVJavaClassExtension = "OptJoinTreeOptOrderingYV";
    private static final String pTOJavaClassExtension = "OptJoinTreeOptOrderingPTO";
    private static final String hashJoinJavaClassExtension = "FindOptJoinTree";
    private static final String hashJoinOnYannakakisJavaClassExtension = "OptJoinTreeOptOrderingHJOnY";

    private GenerateJOBBenchmarkWithOptimalJoinTrees()
    {
    }

    private static final AlgorithmSpecificConfiguration ttjHPConfigurationJOB =
            new AlgorithmSpecificConfiguration(JoinOperator.TTJHP,
                    ttjHPJavaClassExtension,
                    hashJoinJavaClassExtension,
                    queryName2Template,
                    "codegen/job/JOBQueriesAutoGen.javat",
                    "QueriesAutoGenAnnotationBlock");
    private static final AlgorithmSpecificConfiguration ttjHPFixedHJOrderingConfigurationJOB =
            new AlgorithmSpecificConfiguration(JoinOperator.TTJHP,
                    ttjHPFixedHJOrderingJavaClassExtension,
                    hashJoinJavaClassExtension,
                    queryName2Template,
                    "codegen/job/JOBQueriesFixedHJOrdering.javat",
                    "QueriesAutoGenAnnotationBlock");
    private static final AlgorithmSpecificConfiguration ttjHPConfigurationTPCH =
            new AlgorithmSpecificConfiguration(JoinOperator.TTJHP,
                    ttjHPJavaClassExtension,
                    hashJoinJavaClassExtension,
                    TPCHDatabase.CodeGen.queryName2Template,
                    "codegen/tpch/TPCHQueriesAutoGen.javat",
                    "TPCHQueriesAutoGenAnnotationBlock");
    private static final AlgorithmSpecificConfiguration ttjHPConfigurationSSB =
            new AlgorithmSpecificConfiguration(JoinOperator.TTJHP,
                    ttjHPJavaClassExtension,
                    hashJoinJavaClassExtension,
                    SSBDatabase.CodeGen.queryName2Template,
                    "codegen/ssb/SSBQueriesAutoGen.javat",
                    "SSBQueriesAutoGenAnnotationBlock");
    private static final AlgorithmSpecificConfiguration yannakakisConfigurationJOB =
            new AlgorithmSpecificConfiguration(JoinOperator.Yannakakis,
                    yannakakisJavaClassExtension,
                    ttjHPJavaClassExtension,
                    queryName2TempateYannakakis,
                    "codegen/job/JOBQueriesYannakakis.javat",
                    "QueriesAutoGenYannakakisAnnotationBlock");
    private static final AlgorithmSpecificConfiguration yannakakisBConfigurationJOB =
            new AlgorithmSpecificConfiguration(JoinOperator.YannakakisB,
                    yannakakisBJavaClassExtension,
                    ttjHPJavaClassExtension,
                    queryName2TempateYannakakis,
                    "codegen/job/JOBQueriesYannakakisB.javat",
                    "QueriesAutoGenYannakakisBAnnotationBlock");
    private static final AlgorithmSpecificConfiguration yannakakisConfigurationTPCH =
            new AlgorithmSpecificConfiguration(JoinOperator.Yannakakis,
                    yannakakisJavaClassExtension,
                    ttjHPJavaClassExtension,
                    TPCHDatabase.CodeGen.queryName2TempateYannakakis,
                    "codegen/tpch/TPCHQueriesYannakakis.javat",
                    "QueriesAutoGenYannakakisAnnotationBlock");
    private static final AlgorithmSpecificConfiguration yannakakisBConfigurationTPCH =
            new AlgorithmSpecificConfiguration(JoinOperator.YannakakisB,
                    yannakakisBJavaClassExtension,
                    ttjHPJavaClassExtension,
                    TPCHDatabase.CodeGen.queryName2TempateYannakakisB,
                    "codegen/tpch/TPCHQueriesYannakakisB.javat",
                    "QueriesAutoGenYannakakisBAnnotationBlock");
    // NOTE: this is only used for BenchmarkHvTTPCHHJOnOtherOrdering, not for benchmarking
    // Thus, one may need to double check generated benchmark driver to ensure this is not included
    private static final AlgorithmSpecificConfiguration hashJoinOnYannakakisConfigurationTPCH =
            new AlgorithmSpecificConfiguration(JoinOperator.HASH_JOIN,
                    hashJoinOnYannakakisJavaClassExtension,
                    ttjHPJavaClassExtension,
                    TPCHDatabase.CodeGen.queryName2TempateHJOnYannakakis,
                    "codegen/tpch/TPCHQueriesHashJoinOnYannakakis.javat",
                    "QueriesAutoGenHJOnYannakakisAnnotationBlock",
                    JoinOperator.YannakakisB);
    private static final AlgorithmSpecificConfiguration pTOConfigurationTPCH =
            new AlgorithmSpecificConfiguration(JoinOperator.PTO,
                    pTOJavaClassExtension,
                    ttjHPJavaClassExtension,
                    TPCHDatabase.CodeGen.queryName2TempateYannakakisB,
                    "codegen/tpch/TPCHQueriesPTO.javat",
                    "QueriesAutoGenPTOAnnotationBlock");
    private static final AlgorithmSpecificConfiguration yannakakisVanillaConfigurationTPCH =
            new AlgorithmSpecificConfiguration(JoinOperator.YannakakisVanilla,
                    yannakakisVJavaClassExtension,
                    ttjHPJavaClassExtension,
                    TPCHDatabase.CodeGen.queryName2TempateYannakakisB,
                    "codegen/tpch/TPCHQueriesYannakakisV.javat",
                    "QueriesAutoGenYannakakisVAnnotationBlock");
    // TODO: we can merge GenerateFindOptJoinTree code logic to this
    private static final AlgorithmSpecificConfiguration hashJoinConfiguration =
            new AlgorithmSpecificConfiguration(JoinOperator.HASH_JOIN,
                    hashJoinJavaClassExtension,
                    "",
                    null,
                    null,
                    "QueriesFindOptJoinTreeAutoGenAnnotationBlock");

    private static void run(String planStatististicsPath,
                            Map<String, String> relation2MultiwayJoinNode,
                            String templatePathPrefix,
                            String savePathPrefix,
                            Map<String, String> queryName2SavePath,
                            String importPrefix,
                            String benchmarkTemplatePath,
                            List<AlgorithmSpecificConfiguration> algorithmSpecificConfigurations)
            throws IOException, ParseException
    {
        Map<JoinOperator, AlgorithmSpecificConfiguration> algorithmSpecificConfigurationMap =
                algorithmSpecificConfigurations.stream().collect(Collectors.toMap(AlgorithmSpecificConfiguration::getAlgorithm, Function.identity()));
        step1Driver(planStatististicsPath,
                relation2MultiwayJoinNode, templatePathPrefix,
                savePathPrefix, queryName2SavePath, algorithmSpecificConfigurationMap);
        step2Driver(importPrefix, queryName2SavePath, savePathPrefix, algorithmSpecificConfigurationMap);
        step3Driver(benchmarkTemplatePath, savePathPrefix, algorithmSpecificConfigurationMap);
    }

    public static void generateJOBBenchmarkWithOptimalJoinTrees()
            throws IOException, ParseException
    {
        run(JOB_WITH_PREDICATES_RESULT_STORED_PATH,
                relation2MultiwayJoinNode,
                templatePathPrefix,
                savePathPrefix,
                queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.job",
                "codegen/job/BenchmarkJOBWithPredicatesDifferentOrdering.javat",
                List.of(ttjHPConfigurationJOB, yannakakisConfigurationJOB, yannakakisBConfigurationJOB));
    }

    public static void generateJOBBenchmarkWithOptimalJoinTreesFixedHJOrdering()
            throws IOException, ParseException
    {
        run(JOB_WITH_PREDICATES_RESULT_SAME_ORDERING_STORED_PATH,
                relation2MultiwayJoinNode,
                templatePathPrefix,
                savePathPrefix,
                queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.job",
                "codegen/job/BenchmarkJOBWithPredicatesFixedHJOrdering.javat",
                List.of(ttjHPFixedHJOrderingConfigurationJOB));
    }

    public static void generateTPCHBenchmarkWithOptimalJoinTrees()
            throws IOException, ParseException
    {
        run(TPCH_WITH_PREDICATES_RESULT_STORED_PATH,
                TPCHDatabase.CodeGen.relation2MultiwayJoinNode,
                TPCHDatabase.CodeGen.templatePathPrefix,
                TPCHDatabase.CodeGen.savePathPrefix,
                TPCHDatabase.CodeGen.queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.tpch",
                "codegen/tpch/BenchmarkTPCHWithPredicatesDifferentOrdering.javat",
                List.of(ttjHPConfigurationTPCH,
                        yannakakisBConfigurationTPCH,
                        yannakakisConfigurationTPCH,
                        yannakakisVanillaConfigurationTPCH,
                        pTOConfigurationTPCH,
                        hashJoinOnYannakakisConfigurationTPCH));
    }

    public static void generateSSBBenchmarkWithOptimalJoinTrees()
            throws IOException, ParseException
    {
        run(SSB_RESULT_STORED_PATH,
                SSBDatabase.CodeGen.relation2MultiwayJoinNode,
                SSBDatabase.CodeGen.templatePathPrefix,
                SSBDatabase.CodeGen.savePathPrefix,
                SSBDatabase.CodeGen.queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.ssb",
                "codegen/ssb/BenchmarkSSBWithPredicatesDifferentOrdering.javat",
                List.of(ttjHPConfigurationSSB));
    }

    private static class AlgorithmSpecificConfiguration
    {
        @Getter
        private final JoinOperator algorithm;
        private final String javaClassNameExtension;
        private final String planStatisticsFileExtension;
        private List<String> queryNames;
        private final Map<String, String> queryName2Templates;
        private final String queriesAutoGenTemplatePath;
        private final String autoGenAnnotationBlockName;
        // help to determine which planStatistics to use;
        private final JoinOperator planIdentifierJoinOperator;

        public AlgorithmSpecificConfiguration(JoinOperator algorithm,
                                              String javaClassNameExtension,
                                              String planStatisticsFileExtension,
                                              Map<String, String> queryName2Templates,
                                              String queriesAutoGenTemplatePath,
                                              String autoGenAnnotationBlockName)
        {
            this.algorithm = algorithm;
            this.javaClassNameExtension = javaClassNameExtension;
            this.planStatisticsFileExtension = planStatisticsFileExtension;
            this.queryNames = new ArrayList<>();
            this.queryName2Templates = queryName2Templates;
            this.queriesAutoGenTemplatePath = queriesAutoGenTemplatePath;
            this.autoGenAnnotationBlockName = autoGenAnnotationBlockName;
            this.planIdentifierJoinOperator = algorithm;
        }

        public AlgorithmSpecificConfiguration(JoinOperator algorithm,
                                              String javaClassNameExtension,
                                              String planStatisticsFileExtension,
                                              Map<String, String> queryName2Templates,
                                              String queriesAutoGenTemplatePath,
                                              String autoGenAnnotationBlockName,
                                              JoinOperator planIdentifierJoinOperator)
        {
            this.algorithm = algorithm;
            this.javaClassNameExtension = javaClassNameExtension;
            this.planStatisticsFileExtension = planStatisticsFileExtension;
            this.queryNames = new ArrayList<>();
            this.queryName2Templates = queryName2Templates;
            this.queriesAutoGenTemplatePath = queriesAutoGenTemplatePath;
            this.autoGenAnnotationBlockName = autoGenAnnotationBlockName;
            this.planIdentifierJoinOperator = planIdentifierJoinOperator;
        }
    }

    public static void main(String[] args)
            throws IOException, ParseException
    {
        generateJOBBenchmarkWithOptimalJoinTrees();
        generateTPCHBenchmarkWithOptimalJoinTrees();
        generateSSBBenchmarkWithOptimalJoinTrees();
        generateJOBBenchmarkWithOptimalJoinTreesFixedHJOrdering();
    }

    public static class Step1
    {
        /**
         * Generate all the *OptJoinTreeOptOrdering.java from the planStatistics jsons specified in the properties file
         * of propertyFilePath
         */
        static void step1Driver(String planStatisticsPath,
                                Map<String, String> relation2MultiwayJoinNode,
                                String templatePathPrefix,
                                String savePathPrefix,
                                Map<String, String> queryName2SavePath,
                                Map<JoinOperator, AlgorithmSpecificConfiguration> configurations)
                throws IOException, ParseException
        {
            checkState(configurations.containsKey(JoinOperator.TTJHP),
                    "input configurations don't contain TTJHP");
            for (JoinOperator joinOperator : configurations.keySet()) {
                File[] files = getAListOfTargetJsonFiles(planStatisticsPath, configurations.get(joinOperator));
                for (File jsonFile : files) {
                    System.out.println("processing " + jsonFile.getName() + " ...");
                    DataFields jsonFields = extractFields(jsonFile, configurations.get(joinOperator).planStatisticsFileExtension,
                            relation2MultiwayJoinNode);
                    createOptJoinTreeOptOrderingClass(relation2MultiwayJoinNode, templatePathPrefix,
                            savePathPrefix, queryName2SavePath, jsonFields, configurations.get(joinOperator));
                    configurations.get(joinOperator).queryNames.add(jsonFields.queryName);
                }
            }
        }

        private static File[] getAListOfTargetJsonFiles(String planStatisticsPath, AlgorithmSpecificConfiguration configuration)
        {
            File[] returnFiles = new File[] {};
            List<String> patterns = new ArrayList<>(List.of("planStatistics"));
            // JoinOperator.YannakakisB, JoinOperator.YannakakisVanilla, JoinOperator.Yannakakis, and JoinOperator.PTO share the same planStatistics
            if (configuration.planIdentifierJoinOperator == JoinOperator.YannakakisB ||
                    configuration.planIdentifierJoinOperator == JoinOperator.YannakakisVanilla ||
                    configuration.planIdentifierJoinOperator == JoinOperator.PTO) {
                patterns.add(JoinOperator.Yannakakis.name());
            }
            else {
                patterns.add(configuration.planIdentifierJoinOperator.name());
            }
            patterns.add(configuration.planStatisticsFileExtension);
            String[] pattern = patterns.toArray(new String[0]);
            File[] foundFiles = getAListOfTargetJsonFilesFromPattern(planStatisticsPath, pattern);
            returnFiles = (File[]) ArrayUtils.addAll(returnFiles, foundFiles);
            return returnFiles;
        }

        private static File[] getAListOfTargetJsonFilesFromPattern(String planStatisticsPath, String[] pattern)
        {
            return new File(planStatisticsPath).listFiles(new FilenameFilter()
            {
                @Override
                public boolean accept(File dir, String name)
                {
                    return Stream.of(pattern).allMatch(name::contains) && name.endsWith(".json");
                }
            });
        }

        private static DataFields extractFields(File jsonFile,
                                                String planStatisticsFileExtension,
                                                Map<String, String> relation2MultiwayJoinNode)
                throws IOException, ParseException
        {
            return new DataFields(jsonFile.getAbsolutePath(), planStatisticsFileExtension, relation2MultiwayJoinNode);
        }

        private static void createOptJoinTreeOptOrderingClass(Map<String, String> relation2MultiwayJoinNode,
                                                              String templatePathPrefix,
                                                              String savePathPrefix,
                                                              Map<String, String> queryName2SavePath,
                                                              DataFields dataFields,
                                                              AlgorithmSpecificConfiguration configuration)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            context.put("QueryClassName", getJavaClassName(dataFields.queryName, configuration.javaClassNameExtension));
            generateImportSemiJoinOperator(context, configuration);
            generateOptimalJoinTree(context, dataFields.optimalJoinTree);
            List<String> multiwayJoinNodes = generateTraversalListCodeBlock(context, dataFields.relationNames, relation2MultiwayJoinNode);
            generateOrderedGraphCodeBlock(context, dataFields.optimalJoinTree, relation2MultiwayJoinNode);
            generateVerifyJoinOrderingCodeBlock(context, multiwayJoinNodes);
            generateSemiJoinOrderingCodeBlock(context, dataFields.optimalJoinTree, dataFields.bottomUpPassSemijoinOrdering,
                    relation2MultiwayJoinNode);
            generateCreatePhysicalPlanForYannakakisBlock(context, configuration);
            generateJavaClass(context, configuration.javaClassNameExtension,
                    dataFields.queryName, templatePathPrefix, configuration.queryName2Templates, savePathPrefix, queryName2SavePath);
        }

        private static void generateImportSemiJoinOperator(Map<String, Object> context, AlgorithmSpecificConfiguration configuration)
        {
            switch (configuration.algorithm) {
                case Yannakakis:
                    context.put("importSemiJoinOperator", "import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;");
                    break;
                case YannakakisB:
                    context.put("importSemiJoinOperator", "import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;");
                    break;
                case YannakakisVanilla:
                    context.put("importSemiJoinOperator", "import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiHashJoinOperator;");
                    break;
                case PTO:
                    context.put("importSemiJoinOperator", "import org.zhu45.treetracker.relational.operator.TupleBasedLeftSemiBloomJoinOperator;");
                    break;
            }
        }

        private static void generateOptimalJoinTree(Map<String, Object> context, String optimalJoinTree)
        {
            String[] branches = optimalJoinTree.split("\n");
            context.put("optimalJoinTree", Joiner.on("\n * ").join(branches));
        }

        private static List<String> generateTraversalListCodeBlock(Map<String, Object> context,
                                                                   List<String> relationNames,
                                                                   Map<String, String> relation2MultiwayJoinNode)
        {
            List<String> multiwayJoinNodes = new ArrayList<>();
            for (String relationName : relationNames) {
                String multiwayJoinNode = requireNonNull(relation2MultiwayJoinNode.get(relationName));
                multiwayJoinNodes.add(multiwayJoinNode);
            }
            StringBuilder builder = new StringBuilder();
            builder.append("List<MultiwayJoinNode> traversalList = List.of(")
                    .append(Joiner.on(", ").join(multiwayJoinNodes))
                    .append(");\n");
            context.put("traversalListCodeBlock", builder.toString());
            return multiwayJoinNodes;
        }

        private static void generateOrderedGraphCodeBlock(Map<String, Object> context,
                                                          String optimalJoinTree,
                                                          Map<String, String> relation2MultiwayJoinNode)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(");
            String[] branches = optimalJoinTree.split("\n");
            HashMap<Integer, String> depth2relation = new HashMap<>();
            List<String> edges = new ArrayList<>();
            for (String branch : branches) {
                int depth = countMatches(branch, "|");
                String relation = getRelation(branch, relation2MultiwayJoinNode);
                depth2relation.put(depth, relation);
                if (depth2relation.containsKey(depth - 1)) {
                    edges.add(String.format("asDirectedEdge(%s, %s)",
                            relation2MultiwayJoinNode.get(depth2relation.get(depth - 1)),
                            relation2MultiwayJoinNode.get(depth2relation.get(depth))));
                }
                else {
                    builder.append(relation2MultiwayJoinNode.get(relation))
                            .append(", ")
                            .append("Arrays.asList(\n" + repeat(" ", 16));
                }
            }
            builder.append(Joiner.on(",\n" + repeat(" ", 16)).join(edges))
                    .append("), traversalList);");
            context.put("orderedGraphCodeBlock", builder.toString());
        }

        private static void generateSemiJoinOrderingCodeBlock(Map<String, Object> context,
                                                              String optimalJoinTree,
                                                              List<Pair<String, String>> bottomUpSemiJoinOrdering,
                                                              Map<String, String> relation2MultiwayJoinNode)
        {
            if (bottomUpSemiJoinOrdering == null) {
                return;
            }
            StringBuilder builder = new StringBuilder();
            builder.append("SemiJoinOrdering semiJoinOrdering = ")
                    .append("new SemiJoinOrdering(")
                    .append("List.of(");
            List<String> pairs = new ArrayList<>();
            for (Pair<String, String> semiJoinPair : bottomUpSemiJoinOrdering) {
                StringBuilder pair = new StringBuilder();
                pair.append("Pair.of(")
                        .append(relation2MultiwayJoinNode.get(semiJoinPair.getKey()))
                        .append(", ")
                        .append(relation2MultiwayJoinNode.get(semiJoinPair.getValue()))
                        .append(")");
                pairs.add(pair.toString());
            }
            builder.append(Joiner.on(", ").join(pairs))
                    .append("), ")
                    .append("orderedGraph);");
            context.put("semiJoinOrderingCodeBlock", builder.toString());
        }

        private static void generateCreatePhysicalPlanForYannakakisBlock(Map<String, Object> context,
                                                                         AlgorithmSpecificConfiguration configuration)
        {
            switch (configuration.algorithm) {
                case Yannakakis:
                    context.put("createPhysicalPlanForYannakakisBlock", "Plan plan = createPhysicalPlanForYannakakis(semiJoinOrdering, TupleBasedLeftSemiHashJoinOperator.class, false, false);");
                    break;
                case YannakakisB:
                    context.put("createPhysicalPlanForYannakakisBlock", "Plan plan = createPhysicalPlanForYannakakis(semiJoinOrdering, TupleBasedLeftSemiBloomJoinOperator.class, false, false);");
                    break;
                case YannakakisVanilla:
                    context.put("createPhysicalPlanForYannakakisBlock", "Plan plan = createPhysicalPlanForYannakakis(semiJoinOrdering, TupleBasedLeftSemiHashJoinOperator.class, true, false);");
                    break;
                case PTO:
                    context.put("createPhysicalPlanForYannakakisBlock", "Plan plan = createPhysicalPlanForYannakakis(semiJoinOrdering, TupleBasedLeftSemiBloomJoinOperator.class, false, true);");
                    break;
                case HASH_JOIN:
                    context.put("createPhysicalPlanForYannakakisBlock", "Plan plan = createPhysicalPlanFromJoinOrdering(getJoinOrderingFromNodes(traversalList)).getKey();");
                    break;
            }
        }

        public static void generateVerifyJoinOrderingCodeBlock(Map<String, Object> context, List<String> multiwayJoinNodes)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("verifyJoinOrdering(plan, Arrays.asList(");
            List<String> schemaTableNames = new ArrayList<>();
            for (String multiwayJoinNode : multiwayJoinNodes) {
                schemaTableNames.add(multiwayJoinNode + ".getSchemaTableName()");
            }
            builder.append(Joiner.on(",\n" + repeat(" ", 16)).join(schemaTableNames))
                    .append("));");
            context.put("verifyJoinOrderingCodeBlock", builder.toString());
        }

        private static void generateJavaClass(Map<String, Object> context,
                                              String javaClassNameExtension,
                                              String queryName,
                                              String templatePathPrefix,
                                              Map<String, String> queryName2Template,
                                              String savePathPrefix,
                                              Map<String, String> queryName2SavePath)
                throws IOException
        {
            String templatePath = Paths.get(templatePathPrefix, requireNonNull(queryName2Template.get(queryName))).toString();
            String savePath = getJavaSavePath(queryName, javaClassNameExtension, savePathPrefix, queryName2SavePath);

            Jinjava jinjava = new Jinjava();
            String template = Resources.toString(Resources.getResource(templatePath), StandardCharsets.UTF_8);
            String output = jinjava.render(template, context);
            FileUtils.writeStringToFile(new File(savePath), output, StandardCharsets.UTF_8);
        }

        private static String getJavaSavePath(String queryName,
                                              String javaClassNameExtension,
                                              String savePathPrefix,
                                              Map<String, String> queryName2SavePath)
        {
            return Paths.get(savePathPrefix, requireNonNull(queryName2SavePath.get(queryName)),
                    getJavaClassName(queryName, javaClassNameExtension) + ".java").toString();
        }

        private static String getRelation(String branch, Map<String, String> relation2MultiwayJoinNode)
        {
            String schemaTableName = branch.split("\\(")[0];
            String[] schemTableNameSplit = schemaTableName.split("\\.");
            String relationRaw = schemTableNameSplit[1];
            return getCanonicalRelation(relationRaw, relation2MultiwayJoinNode);
        }

        /**
         * Map for instance, "q1a_movie_companies" to "movie_companies"
         */
        public static String getCanonicalRelation(String relationRaw, Map<String, String> relation2MultiwayJoinNode)
        {
            List<String> allRelations = new ArrayList<>(relation2MultiwayJoinNode.keySet());
            String candidate = "";
            int candidatesLength = 0;
            for (String relation : allRelations) {
                if (relationRaw.contains(relation) && relation.length() > candidatesLength) {
                    candidate = relation;
                    candidatesLength = relation.length();
                }
            }
            checkState(candidate.length() != 0, "No canonical relation found for " + relationRaw);
            return candidate;
        }

        private static class DataFields
        {
            private final List<String> relationNames;
            private final String optimalJoinTree;
            private final String queryName;
            private final List<Pair<String, String>> bottomUpPassSemijoinOrdering;

            public DataFields(String file,
                              String planStatisticsFileExtension,
                              Map<String, String> relation2MultiwayJoinNode)
                    throws IOException, ParseException
            {
                Object obj = new JSONParser().parse(new FileReader(file));
                JSONObject jo = (JSONObject) obj;
                relationNames = setRelationNames((JSONArray) ((JSONObject) jo.get("optimalJoinOrdering")).get("schemaTableNameList"),
                        relation2MultiwayJoinNode);
                optimalJoinTree = (String) jo.get("optimalJoinTree");
                queryName = setQueryName(file, planStatisticsFileExtension);
                bottomUpPassSemijoinOrdering = setBottomUpPassSemijoinOrdering(jo, relation2MultiwayJoinNode);
            }

            private List<String> setRelationNames(JSONArray schemaTableNameList, Map<String, String> relation2MultiwayJoinNode)
            {
                List<String> relations = new ArrayList<>();
                for (Object object : schemaTableNameList) {
                    JSONObject jsonObject = (JSONObject) object;
                    relations.add(getCanonicalRelation((String) jsonObject.get("tableName"), relation2MultiwayJoinNode));
                }
                return relations;
            }

            /**
             * Assuming "TTJHP_org.zhu45.treetracker.benchmark.job.q1.Query1aFindOptJoinTree_planStatistics.json"
             */
            private String setQueryName(String file, String planStatisticsFileExtension)
            {
                String[] tokens = file.split("\\.");
                String target = tokens[tokens.length - 2];
                int endIndex = target.indexOf(planStatisticsFileExtension + "_planStatistics");
                checkState(endIndex > 0, "Given pattern " + planStatisticsFileExtension + " not found in " + file);
                return target.substring(0, endIndex);
            }

            private List<Pair<String, String>> setBottomUpPassSemijoinOrdering(JSONObject jsonObject,
                                                                               Map<String, String> relation2MultiwayJoinNode)
            {
                List<Pair<String, String>> bottomUpPassSemijoinOrdering = new ArrayList<>();
                JSONArray ruleStatisticsList = (JSONArray) (jsonObject.get("ruleStatisticsList"));
                //TODO: a better way is to enhance how we serialize ruleStatisticsList to make rule name become
                //a field and the actual rule statics its corresponding value, i.e., we don't have to iterate through
                //the ruleStatisticsList and we might have error if two rules contain semijoin ordering.
                for (Object ruleStatisticsObject : ruleStatisticsList) {
                    JSONObject ruleStatistics = (JSONObject) ruleStatisticsObject;
                    Object semijoinOrderingObject = ruleStatistics.get("semiJoinOrdering");
                    if (semijoinOrderingObject != null) {
                        JSONObject semijoinOrdering = (JSONObject) semijoinOrderingObject;
                        JSONArray bottomUpPassOrdering = (JSONArray) semijoinOrdering.get("bottomUpPass");
                        for (Object semijoinPairObject : bottomUpPassOrdering) {
                            JSONObject semijoinPair = (JSONObject) semijoinPairObject;
                            bottomUpPassSemijoinOrdering.add(Pair.of(getCanonicalRelation((String) ((JSONObject) ((JSONObject) semijoinPair.get("left")).get("schemaTableName")).get("tableName"), relation2MultiwayJoinNode),
                                    getCanonicalRelation((String) ((JSONObject) ((JSONObject) semijoinPair.get("right")).get("schemaTableName")).get("tableName"), relation2MultiwayJoinNode)));
                        }
                        break;
                    }
                }
                if (bottomUpPassSemijoinOrdering.isEmpty()) {
                    return null;
                }
                return bottomUpPassSemijoinOrdering;
            }
        }
    }

    static class Step2
    {
        /**
         * Generate JOBQueriesAutoGen class
         */
        static void step2Driver(String importPrefix,
                                Map<String, String> queryName2SavePath,
                                String savePathPrefix,
                                Map<JoinOperator, AlgorithmSpecificConfiguration> configurations)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            for (Map.Entry<JoinOperator, AlgorithmSpecificConfiguration> entry : configurations.entrySet()) {
                generateImportCodeBlock(context, importPrefix, queryName2SavePath, entry.getValue());
                generateEnumCodeBlock(context, entry.getValue());
                generateCaseCodeBlock(context, entry.getValue());
                generateJOBQueriesAutoGen(context, savePathPrefix, entry.getValue());
            }
        }

        private static void generateImportCodeBlock(Map<String, Object> context,
                                                    String importPrefix,
                                                    Map<String, String> queryName2SavePath,
                                                    AlgorithmSpecificConfiguration configuration)
        {
            configuration.queryNames.sort(String::compareTo);
            StringBuilder builder = new StringBuilder();
            List<String> importStatements = new ArrayList<>();
            for (String queryName : configuration.queryNames) {
                List<String> joinCandidates = new ArrayList<>();
                joinCandidates.add(importPrefix);
                if (!requireNonNull(queryName2SavePath.get(queryName)).isEmpty()) {
                    joinCandidates.add(queryName2SavePath.get(queryName));
                }
                joinCandidates.add(getJavaClassName(queryName, configuration.javaClassNameExtension) + ";");
                importStatements.add(Joiner.on(".").join(joinCandidates));
            }
            importStatements.sort(String::compareTo);
            builder.append(Joiner.on("\n").join(importStatements));
            context.put("importCodeBlock", builder.toString());
        }

        private static void generateEnumCodeBlock(Map<String, Object> context,
                                                  AlgorithmSpecificConfiguration configuration)
        {
            StringBuilder builder = new StringBuilder();
            List<String> enums = new ArrayList<>();
            for (String queryName : configuration.queryNames) {
                enums.add(String.format("%s(\"%s\")", getJavaClassName(queryName, configuration.javaClassNameExtension),
                        getJavaClassName(queryName, configuration.javaClassNameExtension)));
            }
            builder.append(Joiner.on(",\n" + repeat(" ", 4)).join(enums))
                    .append(";");
            context.put("EnumCodeBlock", builder.toString());
        }

        private static void generateCaseCodeBlock(Map<String, Object> context,
                                                  AlgorithmSpecificConfiguration configuration)
        {
            StringBuilder builder = new StringBuilder();
            List<String> cases = new ArrayList<>();
            for (String queryName : configuration.queryNames) {
                cases.add(buildACase(queryName, configuration.javaClassNameExtension));
            }
            builder.append(Joiner.on("\n" + repeat(" ", 16)).join(cases));
            context.put("caseCodeBlock", builder.toString());
        }

        private static String buildACase(String queryName, String javaClassNameExtension)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("case ")
                    .append(getJavaClassName(queryName, javaClassNameExtension))
                    .append(":\n");
            builder.append(repeat(" ", 20))
                    .append(String.format("map.put(c.val, %s.class);", getJavaClassName(queryName, javaClassNameExtension)))
                    .append("\n");
            builder.append(repeat(" ", 20))
                    .append("break;");
            return builder.toString();
        }

        private static void generateJOBQueriesAutoGen(Map<String, Object> context,
                                                      String savePathPrefix,
                                                      AlgorithmSpecificConfiguration configuration)
                throws IOException
        {
            Jinjava jinjava = new Jinjava();
            String template = Resources.toString(Resources.getResource(configuration.queriesAutoGenTemplatePath), StandardCharsets.UTF_8);
            String output = jinjava.render(template, context);
            Path queriesAutoGenTempateP = Paths.get(configuration.queriesAutoGenTemplatePath);
            String outputJavaName = FilenameUtils.removeExtension(queriesAutoGenTempateP.getName(queriesAutoGenTempateP.getNameCount() - 1).toString()) + ".java";
            FileUtils.writeStringToFile(new File(Paths.get(savePathPrefix, outputJavaName).toString()),
                    output, StandardCharsets.UTF_8);
        }
    }

    static class Step3
    {
        static void step3Driver(String benchmarkTemplatePath,
                                String savePathPrefix,
                                Map<JoinOperator, AlgorithmSpecificConfiguration> algorithmSpecificConfigurations)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            Map<JoinOperator, AlgorithmSpecificConfiguration> configurations = loadHashJoinConfiguration(algorithmSpecificConfigurations);
            for (AlgorithmSpecificConfiguration configuration : configurations.values()) {
                generateJOBQueriesAutoGenAnnotationBlock(context, configuration);
            }
            generateBenchmarkJOBWithPredicatesAutoGen(context, benchmarkTemplatePath, savePathPrefix);
        }

        private static Map<JoinOperator, AlgorithmSpecificConfiguration> loadHashJoinConfiguration(Map<JoinOperator, AlgorithmSpecificConfiguration> algorithmSpecificConfigurations)
        {
            // TODO: if we merge GenerateFindOptJoinTree into this, we can remove the method.
            Map<JoinOperator, AlgorithmSpecificConfiguration> configurations = new HashMap<>(algorithmSpecificConfigurations);
            hashJoinConfiguration.queryNames = algorithmSpecificConfigurations.get(JoinOperator.TTJHP).queryNames;
            configurations.put(JoinOperator.HASH_JOIN, hashJoinConfiguration);
            return configurations;
        }

        private static void generateJOBQueriesAutoGenAnnotationBlock(Map<String, Object> context,
                                                                     AlgorithmSpecificConfiguration configuration)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("@Param({");
            List<String> queries = new ArrayList<>();
            for (String queryName : configuration.queryNames) {
                queries.add("\"" + getJavaClassName(queryName, configuration.javaClassNameExtension) + "\"");
            }
            stringBuilder.append(Joiner.on(", ").join(queries))
                    .append("})");
            context.put(configuration.autoGenAnnotationBlockName, stringBuilder.toString());
        }

        private static void generateBenchmarkJOBWithPredicatesAutoGen(Map<String, Object> context,
                                                                      String benchmarkTemplatePath,
                                                                      String savePathPrefix)
                throws IOException
        {
            Jinjava jinjava = new Jinjava();
            String template = Resources.toString(Resources.getResource(benchmarkTemplatePath), StandardCharsets.UTF_8);
            String output = jinjava.render(template, context);
            Path benchmarkTemplateP = Paths.get(benchmarkTemplatePath);
            String outputJavaName = FilenameUtils.removeExtension(benchmarkTemplateP.getName(benchmarkTemplateP.getNameCount() - 1).toString()) + ".java";
            FileUtils.writeStringToFile(new File(Paths.get(savePathPrefix, outputJavaName).toString()),
                    output, StandardCharsets.UTF_8);
        }
    }

    private static String getJavaClassName(String queryName, String extension)
    {
        return queryName + extension;
    }
}
