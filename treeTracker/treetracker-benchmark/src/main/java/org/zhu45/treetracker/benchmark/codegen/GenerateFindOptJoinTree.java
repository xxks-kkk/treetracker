package org.zhu45.treetracker.benchmark.codegen;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.json.simple.parser.ParseException;
import org.zhu45.treetracker.benchmark.ssb.SSBDatabase;
import org.zhu45.treetracker.benchmark.statsgen.GenerateOptimalJoinOrderingCSV;
import org.zhu45.treetracker.benchmark.tpch.TPCHDatabase;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang.StringUtils.repeat;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.codegen.GenerateFindOptJoinTree.Step2.step2Driver;
import static org.zhu45.treetracker.benchmark.codegen.GenerateFindOptJoinTree.Step3.step3Driver;
import static org.zhu45.treetracker.benchmark.codegen.GenerateJOBBenchmarkWithOptimalJoinTrees.Step1.generateVerifyJoinOrderingCodeBlock;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2SavePath;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2Template;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.relation2MultiwayJoinNode;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.savePathPrefix;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.templatePathPrefix;
import static org.zhu45.treetracker.benchmark.statsgen.GenerateOptimalJoinOrderingCSV.extractFields;

/**
 * Purpose:
 * Takes in join ordering for HJ and produce corresponding *FindOptJoinTree.java to
 * find the optimal join tree for TTJ given the ordering. We illustrate steps using JOB as an example.
 * <p>
 * Steps:
 * 1. The program finds all the HJ planStatistics.json and extract "optimalJoinOrdering" fields
 * from it. Then, For each extracted ordering, the program creates *FindOptJoinTree.java.
 * 2. After creating all the *FindOptJoinTree.java, the program creates JOBQueriesFindOptJoinTree.java to register
 * those classes to be used in the benchmark.
 * 3. The program creates FindOptimalJoinTreeForJOBWithPredicatesAutoGen.java to drive those queries. User can then run
 * FindOptimalJoinTreeForJOBWithPredicatesAutoGen.java to execute the benchmark to find optimal join trees.
 */
public class GenerateFindOptJoinTree
{
    private GenerateFindOptJoinTree()
    {
    }

    public static class Step1
    {
        /**
         * Generate all the *FindOptJoinTree.java from the HJ planStatistics jsons
         */
        static List<String> step1Driver(String planStatisticsPath,
                                        Map<String, String> relation2MultiwayJoinNode,
                                        String templatePathPrefix,
                                        Map<String, String> queryName2Template,
                                        String savePathPrefix,
                                        Map<String, String> queryName2SavePath)
                throws IOException, ParseException
        {
            File[] files = getAListOfTargetJsonFiles(planStatisticsPath);
            List<String> queryNames = new ArrayList<>();
            for (File jsonFile : files) {
                System.out.println("processing " + jsonFile.getName() + " ...");
                GenerateOptimalJoinOrderingCSV.DataFields jsonFields = extractFields(jsonFile, relation2MultiwayJoinNode);
                createFindOptJoinTreeClass(jsonFields.relationNames,
                        jsonFields.queryName,
                        relation2MultiwayJoinNode,
                        templatePathPrefix,
                        queryName2Template,
                        savePathPrefix,
                        queryName2SavePath);
                queryNames.add(jsonFields.queryName);
            }
            return queryNames;
        }

        private static File[] getAListOfTargetJsonFiles(String planStatisticsPath)
        {
            String[] patterns = "HASH_JOIN,planStatistics".split(",");
            return new File(planStatisticsPath).listFiles((dir, name) -> Stream.of(patterns).allMatch(name::contains) && name.endsWith(".json"));
        }

        private static void createFindOptJoinTreeClass(List<String> relationNames,
                                                       String queryName,
                                                       Map<String, String> relation2MultiwayJoinNode,
                                                       String templatePathPrefix,
                                                       Map<String, String> queryName2Template,
                                                       String savePathPrefix,
                                                       Map<String, String> queryName2SavePath)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            context.put("QueryClassName", getJavaClassName(queryName));
            List<String> multiwayJoinNodes = generateCreatePhysicalPlanFromJoinOrderingCodeBlock(context, relationNames, relation2MultiwayJoinNode);
            generateVerifyJoinOrderingCodeBlock(context, multiwayJoinNodes);
            generateJavaClass(context, queryName, templatePathPrefix, queryName2Template, savePathPrefix, queryName2SavePath);
        }

        private static List<String> generateCreatePhysicalPlanFromJoinOrderingCodeBlock(Map<String, Object> context,
                                                                                        List<String> relationNames,
                                                                                        Map<String, String> relation2MultiwayJoinNode)
        {
            StringBuilder stringBuilder = new StringBuilder();
            List<String> multiwayJoinNodes = relationNames.stream().map(relation2MultiwayJoinNode::get).collect(Collectors.toList());
            stringBuilder.append("Pair<Plan, List<Operator>> pair = createPhysicalPlanFromJoinOrdering(")
                    .append("getJoinOrderingFromNodes(List.of(")
                    .append(Joiner.on(", ").join(multiwayJoinNodes))
                    .append(")));\n");
            stringBuilder.append(" ".repeat(8))
                    .append("Plan plan = pair.getKey();")
                    .append("\n");
            context.put("createPhysicalPlanFromJoinOrderingCodeBlock", stringBuilder.toString());
            return multiwayJoinNodes;
        }

        private static void generateJavaClass(Map<String, Object> context,
                                              String queryName,
                                              String templatePathPrefix,
                                              Map<String, String> queryName2Template,
                                              String savePathPrefix,
                                              Map<String, String> queryName2SavePath)
                throws IOException
        {
            String templatePath = Paths.get(templatePathPrefix, constructTemplateName(queryName, queryName2Template)).toString();
            String savePath = getJavaSavePath(queryName, savePathPrefix, queryName2SavePath);

            Jinjava jinjava = new Jinjava();
            String template = Resources.toString(Resources.getResource(templatePath), StandardCharsets.UTF_8);
            String output = jinjava.render(template, context);
            FileUtils.writeStringToFile(new File(savePath), output, StandardCharsets.UTF_8);
        }

        private static String constructTemplateName(String queryName, Map<String, String> queryName2Template)
        {
            String queryTemplateNameForOptJoinTreeOptOrdering = requireNonNull(queryName2Template.get(queryName));
            String templateNameWithoutExtension = queryTemplateNameForOptJoinTreeOptOrdering.split("\\.")[0];
            return templateNameWithoutExtension + "FindOptJoinTree" + ".javat";
        }

        private static String getJavaSavePath(String queryName, String savePathPrefix, Map<String, String> queryName2SavePath)
        {
            return Paths.get(savePathPrefix, requireNonNull(queryName2SavePath.get(queryName)),
                    getJavaClassName(queryName) + ".java").toString();
        }
    }

    static class Step2
    {
        /**
         * Generate JOBQueriesFindOptJoinTree.javat class
         */
        static void step2Driver(List<String> queryNames,
                                Map<String, String> queryName2SavePath,
                                String importPrefix,
                                String savePathPrefix,
                                String queriesFindOptJoinTreeTemplatePath)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            generateImportCodeBlock(context, queryNames, queryName2SavePath, importPrefix);
            generateEnumCodeBlock(context, queryNames);
            generateCaseCodeBlock(context, queryNames);
            generateQueriesAutoGen(context, savePathPrefix, queriesFindOptJoinTreeTemplatePath);
        }

        private static void generateImportCodeBlock(Map<String, Object> context, List<String> queryNames,
                                                    Map<String, String> queryName2SavePath,
                                                    String importPrefix)
        {
            queryNames.sort(String::compareTo);
            StringBuilder builder = new StringBuilder();
            List<String> importStatements = new ArrayList<>();
            for (String queryName : queryNames) {
                importStatements.add(Joiner.on(".").join(importPrefix,
                        requireNonNull(queryName2SavePath.get(queryName)),
                        getJavaClassName(queryName) + ";"));
            }
            importStatements.sort(String::compareTo);
            builder.append(Joiner.on("\n").join(importStatements));
            context.put("importCodeBlock", builder.toString());
        }

        private static void generateEnumCodeBlock(Map<String, Object> context, List<String> queryNames)
        {
            StringBuilder builder = new StringBuilder();
            List<String> enums = new ArrayList<>();
            for (String queryName : queryNames) {
                enums.add(String.format("%s(\"%s\")", getJavaClassName(queryName), getJavaClassName(queryName)));
            }
            builder.append(Joiner.on(",\n" + repeat(" ", 4)).join(enums))
                    .append(";");
            context.put("EnumCodeBlock", builder.toString());
        }

        private static void generateCaseCodeBlock(Map<String, Object> context, List<String> queryNames)
        {
            StringBuilder builder = new StringBuilder();
            List<String> cases = new ArrayList<>();
            for (String queryName : queryNames) {
                cases.add(buildACase(queryName));
            }
            builder.append(Joiner.on("\n" + repeat(" ", 16)).join(cases));
            context.put("caseCodeBlock", builder.toString());
        }

        private static String buildACase(String queryName)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("case ")
                    .append(getJavaClassName(queryName))
                    .append(":\n");
            builder.append(repeat(" ", 20))
                    .append(String.format("map.put(c.val, %s.class);", getJavaClassName(queryName)))
                    .append("\n");
            builder.append(repeat(" ", 20))
                    .append("break;");
            return builder.toString();
        }

        private static void generateQueriesAutoGen(Map<String, Object> context, String savePathPrefix, String queriesFindOptJoinTreeTemplatePath)
                throws IOException
        {
            Jinjava jinjava = new Jinjava();
            String template = Resources.toString(Resources.getResource(queriesFindOptJoinTreeTemplatePath), StandardCharsets.UTF_8);
            String output = jinjava.render(template, context);
            Path queriesFindOptJoinTreeTempateP = Paths.get(queriesFindOptJoinTreeTemplatePath);
            String outputJavaName = FilenameUtils.removeExtension(queriesFindOptJoinTreeTempateP.getName(queriesFindOptJoinTreeTempateP.getNameCount() - 1).toString()) + ".java";
            FileUtils.writeStringToFile(new File(Paths.get(savePathPrefix, outputJavaName).toString()),
                    output, StandardCharsets.UTF_8);
        }
    }

    static class Step3
    {
        static void step3Driver(List<String> queryNames, String autoGenAnnotationBlockName,
                                String savePathPrefix, String benchmarkTemplatePath)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            generateQueriesAutoGenAnnotationBlock(context, queryNames, autoGenAnnotationBlockName);
            generateBenchmarkJOBWithPredicatesAutoGen(context, savePathPrefix, benchmarkTemplatePath);
        }

        private static void generateQueriesAutoGenAnnotationBlock(Map<String, Object> context, List<String> queryNames,
                                                                  String autoGenAnnotationBlockName)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("@Param({");
            List<String> queries = new ArrayList<>();
            for (String queryName : queryNames) {
                queries.add("\"" + getJavaClassName(queryName) + "\"");
            }
            stringBuilder.append(Joiner.on(", ").join(queries))
                    .append("})");
            context.put(autoGenAnnotationBlockName, stringBuilder.toString());
        }

        private static void generateBenchmarkJOBWithPredicatesAutoGen(Map<String, Object> context,
                                                                      String savePathPrefix,
                                                                      String benchmarkTemplatePath)
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

    private static String getJavaClassName(String queryName)
    {
        return queryName + "FindOptJoinTree";
    }

    private static void run(String planStatisticsPath,
                            Map<String, String> relation2MultiwayJoinNode,
                            String templatePathPrefix,
                            Map<String, String> queryName2Template,
                            String savePathPrefix,
                            Map<String, String> queryName2SavePath,
                            String importPrefix,
                            String queriesFindOptJoinTreeTemplatePath,
                            String autoGenAnnotationBlockName,
                            String benchmarkTemplatePath)
            throws IOException, ParseException
    {
        List<String> queryNames = Step1.step1Driver(planStatisticsPath, relation2MultiwayJoinNode,
                templatePathPrefix, queryName2Template, savePathPrefix, queryName2SavePath);
        step2Driver(queryNames, queryName2SavePath, importPrefix, savePathPrefix, queriesFindOptJoinTreeTemplatePath);
        step3Driver(queryNames, autoGenAnnotationBlockName, savePathPrefix, benchmarkTemplatePath);
    }

    public static void generateFindOptJoinTreeJOB()
            throws IOException, ParseException
    {
        run(JOB_WITH_PREDICATES_RESULT_STORED_PATH,
                relation2MultiwayJoinNode,
                templatePathPrefix,
                queryName2Template,
                savePathPrefix,
                queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.job",
                "codegen/job/JOBQueriesFindOptJoinTree.javat",
                "JOBQueriesAutoGenAnnotationBlock",
                "codegen/job/FindOptimalJoinTreeForJOBWithPredicatesAutoGen.javat");
    }

    public static void generateFindOptJoinTreeTPCH()
            throws IOException, ParseException
    {
        run(TPCH_WITH_PREDICATES_RESULT_STORED_PATH,
                TPCHDatabase.CodeGen.relation2MultiwayJoinNode,
                TPCHDatabase.CodeGen.templatePathPrefix,
                TPCHDatabase.CodeGen.queryName2Template,
                TPCHDatabase.CodeGen.savePathPrefix,
                TPCHDatabase.CodeGen.queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.tpch",
                "codegen/tpch/TPCHQueriesFindOptJoinTree.javat",
                "TPCHQueriesAutoGenAnnotationBlock",
                "codegen/tpch/FindOptimalJoinTreeForTPCHWithPredicatesAutoGen.javat");
    }

    public static void generateFindOptJoinTreeSSB()
            throws IOException, ParseException
    {
        run(SSB_RESULT_STORED_PATH,
                SSBDatabase.CodeGen.relation2MultiwayJoinNode,
                SSBDatabase.CodeGen.templatePathPrefix,
                SSBDatabase.CodeGen.queryName2Template,
                SSBDatabase.CodeGen.savePathPrefix,
                SSBDatabase.CodeGen.queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.ssb",
                "codegen/ssb/SSBQueriesFindOptJoinTree.javat",
                "SSBQueriesAutoGenAnnotationBlock",
                "codegen/ssb/FindOptimalJoinTreeForSSBWithPredicatesAutoGen.javat");
    }

    public static void main(String[] args)
            throws IOException, ParseException
    {
        generateFindOptJoinTreeJOB();
        generateFindOptJoinTreeTPCH();
        generateFindOptJoinTreeSSB();
    }
}
