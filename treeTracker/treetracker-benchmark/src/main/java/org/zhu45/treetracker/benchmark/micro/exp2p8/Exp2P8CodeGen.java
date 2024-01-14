package org.zhu45.treetracker.benchmark.micro.exp2p8;

import com.google.common.base.Joiner;
import com.google.common.io.Resources;
import com.hubspot.jinjava.Jinjava;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.commons.lang.StringUtils.repeat;
import static org.zhu45.treetracker.benchmark.Benchmarks.EXP2P8_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8CodeGen.DatabaseGen.generateDatabase;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8CodeGen.QueryEnumGen.generateQueryEnum;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8CodeGen.QueryGen.generateQuery;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8CodeGen.SQLScriptGen.generateSQLScript;
import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8DataGen2.generateData;

/**
 * The main driver for Exp 2.8: Impact of Backjumping
 * <p>
 * The control parameter is:
 * 1) the number of backjumped relations
 * 2) the backjumped relation size for a fixed number of backjumped relations
 * <p>
 * The workflow is following:
 * 1. for a fixed parameter combination, we first generate data (see Exp2P8DataGen)
 * 2. we generate benchmark queries, driver, and corresponding SQL data ingestion script
 * 3. Run the SQL data ingestion script
 * 4. Run the benchmark
 */
public class Exp2P8CodeGen
{
    private Exp2P8CodeGen()
    {
    }

    private static void render(Map<String, Object> context,
                               String templatePath,
                               String savePath)
            throws IOException
    {
        Jinjava jinjava = new Jinjava();
        String template = Resources.toString(Resources.getResource(templatePath), StandardCharsets.UTF_8);
        String output = jinjava.render(template, context);
        FileUtils.writeStringToFile(new File(savePath), output, StandardCharsets.UTF_8);
    }

    static class SQLScriptGen
    {
        static void generateSQLScript(List<GenerateGroup> generateGroups)
                throws IOException
        {
            String sqlTemplatePath = Paths.get("codegen", "exp2p8", "exp2p8-duckdb.sqlt").toString();
            String savePath = Paths.get("treetracker-benchmark",
                    "src", "main", "resources", "exp2p8", "exp2p8-duckdb.sql").toString();
            Map<String, Object> context = new HashMap<>();
            List<String> createStatements = new ArrayList<>();
            List<String> copyStatements = new ArrayList<>();
            for (GenerateGroup generateGroup : generateGroups) {
                createStatements.addAll(generateCreateStatements(generateGroup));
                copyStatements.addAll(generateCopyDataBlocks(generateGroup));
            }
            buildCreateStatements(context, createStatements);
            buildCopyDataBlocks(context, copyStatements);
            render(context, sqlTemplatePath, savePath);
        }

        private static List<String> generateCreateStatements(GenerateGroup generateGroup)
        {
            List<String> createStatements = new ArrayList<>();
            LinkedHashMap<String, String> relation2CSVFileName = new LinkedHashMap<>();
            List<String> relations = new ArrayList<>();
            for (int i = 0; i < generateGroup.csvFiles.size(); i++) {
                String csvFileName = generateGroup.csvFiles.get(i);
                List<String> attributes = getAttributesFromCSVFileName(csvFileName);
                String relation = getTableNameFromCSVFileName(csvFileName, i);
                relations.add(relation);
                relation2CSVFileName.put(relation, csvFileName);
                createStatements.add(String.format("CREATE TABLE exp2p8.%s (%s);",
                        relation,
                        Joiner.on(",").join(attributes.stream().map(attr -> String.format("%s integer", attr)).collect(Collectors.toList()))));
            }
            generateGroup.setRelations(relations);
            generateGroup.setRelation2CSVFileName(relation2CSVFileName);
            return createStatements;
        }

        private static void buildCreateStatements(Map<String, Object> context,
                                                  List<String> createStatements)
        {
            context.put("CreateTableBlock", Joiner.on("\n").join(createStatements));
        }

        /**
         * We assume "exp2.8a1a5_3_2.csv" and returns [a1, a5]
         */
        private static List<String> getAttributesFromCSVFileName(String csvFileName)
        {
            String[] parts = csvFileName.split("_");
            String removedExp2p8Prefix = parts[0].substring(6);
            List<Integer> indexes = IntStream
                    .iterate(removedExp2p8Prefix.indexOf("a"), index -> index >= 0, index -> removedExp2p8Prefix.indexOf("a", index + 1))
                    .boxed()
                    .collect(Collectors.toList());
            List<String> attributes = new ArrayList<>();
            for (int i = 0; i < indexes.size(); ++i) {
                if (i != indexes.size() - 1) {
                    attributes.add(removedExp2p8Prefix.substring(indexes.get(i), indexes.get(i + 1)));
                }
                else {
                    attributes.add(removedExp2p8Prefix.substring(indexes.get(i)));
                }
            }
            return attributes;
        }

        /**
         * We assume "exp2.5a1a5_3_2.csv" and returns "T0k3n2"
         */
        private static String getTableNameFromCSVFileName(String csvFileName, int i)
        {
            String[] parts = csvFileName.split("_");
            return "t" + i + "k" + parts[1] + "n" + parts[2].split("\\.")[0];
        }

        private static List<String> generateCopyDataBlocks(GenerateGroup generateGroup)
        {
            List<String> copyStatements = new ArrayList<>();
            for (String relation : generateGroup.relation2CSVFileName.keySet()) {
                String csvFileName = generateGroup.relation2CSVFileName.get(relation);
                copyStatements.add(String.format("COPY exp2p8.%s FROM '%s' WITH DELIMITER AS ',' NULL AS '' QUOTE AS '\"' ESCAPE AS '\\' CSV;",
                        relation,
                        Paths.get(EXP2P8_RESULT_STORED_PATH, csvFileName)));
            }
            return copyStatements;
        }

        private static void buildCopyDataBlocks(Map<String, Object> context,
                                                List<String> copyStatements)
        {
            context.put("CopyDataBlock", Joiner.on("\n").join(copyStatements));
        }
    }

    /**
     * Generate Exp2P8Database
     */
    static class DatabaseGen
    {
        static void generateDatabase(List<GenerateGroup> generateGroups)
                throws IOException
        {
            String templatePath = Paths.get("codegen", "exp2p8", "Exp2P8Database.javat").toString();
            String savePath = Paths.get("treetracker-benchmark", "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "micro", "exp2p8", "Exp2P8Database.java").toString();
            Map<String, Object> context = new HashMap<>();
            List<String> relationStatements = new ArrayList<>();
            for (GenerateGroup generateGroup : generateGroups) {
                for (String relation : generateGroup.relations) {
                    relationStatements.add(String.format("public static String %s = \"%s\";", relation, relation));
                }
            }
            context.put("relationBlock", Joiner.on("\n    ").join(relationStatements));
            render(context, templatePath, savePath);
        }
    }

    /**
     * Generate Java Query class
     */
    static class QueryGen
    {
        static String joinTree1 = "JoinTree1";
        static String joinTree2 = "JoinTree2";

        public static void generateQuery(List<GenerateGroup> generateGroups)
                throws IOException
        {
            for (GenerateGroup generateGroup : generateGroups) {
                generateQueryHelper(joinTree1, generateGroup);
                generateQueryHelper(joinTree2, generateGroup);
            }
        }

        static void generateQueryHelper(String joinTreeShape, GenerateGroup generateGroup)
                throws IOException
        {
            String templatePath = Paths.get("codegen", "exp2p8", "Exp2P8JoinTreeQuery.javat").toString();
            String savePath = getJavaSavePath(joinTreeShape, generateGroup);
            Map<String, Object> context = new HashMap<>();
            context.put("numberOfBackJumpedRelations", generateGroup.numberOfBackJumpedRelations);
            context.put("backjumpedRelationSize", generateGroup.backjumpedRelationSize);
            context.put("JoinTreeShape", joinTreeShape);
            generateImportRelationsBlock(context, generateGroup);
            generateMultiwayJoinNodeCodeBlock(context, generateGroup);
            generateTraversalList(context, generateGroup);
            if (Objects.equals(joinTreeShape, joinTree1)) {
                generateOrderedGraphCodeBlockJoinTree1(context, generateGroup);
            }
            else if (Objects.equals(joinTreeShape, joinTree2)) {
                generateOrderedGraphCodeBlockJoinTree2(context, generateGroup);
            }
            generateVerifyJoinOrderingCodeBlock(context, generateGroup);
            render(context, templatePath, savePath);
        }

        private static void generateImportRelationsBlock(Map<String, Object> context,
                                                         GenerateGroup generateGroup)
        {
            List<String> importStatements = new ArrayList<>();
            for (String relation : generateGroup.relations) {
                importStatements.add(String.format("import static org.zhu45.treetracker.benchmark.micro.exp2p8.Exp2P8Database.%s;", relation));
            }
            Collections.sort(importStatements);
            context.put("importRelaitonsBlock", Joiner.on("\n").join(importStatements));
        }

        private static void generateMultiwayJoinNodeCodeBlock(Map<String, Object> context,
                                                              GenerateGroup generateGroup)
        {
            List<String> multiwayJoinNodeStatements = new ArrayList<>();
            List<String> multiwayJoinNodes = new ArrayList<>();
            for (String relation : generateGroup.relations) {
                String multwayJoinNode = relation + "Node";
                multiwayJoinNodes.add(multwayJoinNode);
                multiwayJoinNodeStatements.add(String.format("MultiwayJoinNode %s = getTableNode(%s);", multwayJoinNode, relation));
            }
            context.put("multiwayJoinNodeCodeBlock", Joiner.on("\n        ").join(multiwayJoinNodeStatements));
            generateGroup.multiwayJoinNodes = multiwayJoinNodes;
        }

        private static void generateTraversalList(Map<String, Object> context,
                                                  GenerateGroup generateGroup)
        {
            context.put("traversalListCodeBlock", String.format("List<MultiwayJoinNode> traversalList = List.of(%s);",
                    Joiner.on(", ").join(generateGroup.multiwayJoinNodes)));
        }

        private static void generateOrderedGraphCodeBlockJoinTree1(Map<String, Object> context,
                                                                   GenerateGroup generateGroup)
        {
            String codeBlock = "MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(%s, Arrays.asList(%s), traversalList);";
            List<String> asDirectedEdgeList = new ArrayList<>();
            for (int i = 0; i < generateGroup.multiwayJoinNodes.size() - 1; i++) {
                asDirectedEdgeList.add(String.format("asDirectedEdge(%s, %s)",
                        generateGroup.multiwayJoinNodes.get(i),
                        generateGroup.multiwayJoinNodes.get(i + 1)));
            }
            context.put("orderedGraphCodeBlock", String.format(codeBlock, generateGroup.multiwayJoinNodes.get(0), Joiner.on(", ").join(asDirectedEdgeList)));
        }

        private static void generateOrderedGraphCodeBlockJoinTree2(Map<String, Object> context,
                                                                   GenerateGroup generateGroup)
        {
            String codeBlock = "MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(%s, Arrays.asList(%s), traversalList);";
            List<String> asDirectedEdgeList = new ArrayList<>();
            for (int i = 1; i < generateGroup.multiwayJoinNodes.size(); i++) {
                asDirectedEdgeList.add(String.format("asDirectedEdge(%s, %s)",
                        generateGroup.multiwayJoinNodes.get(0),
                        generateGroup.multiwayJoinNodes.get(i)));
            }
            context.put("orderedGraphCodeBlock", String.format(codeBlock, generateGroup.multiwayJoinNodes.get(0), Joiner.on(", ").join(asDirectedEdgeList)));
        }

        private static void generateVerifyJoinOrderingCodeBlock(Map<String, Object> context,
                                                                GenerateGroup generateGroup)
        {
            String codeBlock = "verifyJoinOrdering(plan, Arrays.asList(%s));";
            List<String> schemaTableNames = new ArrayList<>();
            for (String multiwayJoinNode : generateGroup.multiwayJoinNodes) {
                schemaTableNames.add(String.format("%s.getSchemaTableName()", multiwayJoinNode));
            }
            context.put("verifyJoinOrderingCodeBlock", String.format(codeBlock, Joiner.on(", ").join(schemaTableNames)));
        }

        private static String getJavaSavePath(String joinTreeShape,
                                              GenerateGroup generateGroup)
        {
            String javaClass = String.format("Exp2P8%sQueryK%sN%s", joinTreeShape, generateGroup.numberOfBackJumpedRelations, generateGroup.backjumpedRelationSize);
            generateGroup.benchmarkQueries.add(javaClass);
            return Paths.get("treetracker-benchmark", "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "micro", "exp2p8", javaClass + ".java").toString();
        }
    }

    static class QueryEnumGen
    {
        public static void generateQueryEnum(List<GenerateGroup> generateGroups)
                throws IOException
        {
            Map<String, Object> context = new HashMap<>();
            String templatePath = Paths.get("codegen", "exp2p8", "Exp2P8Queries.javat").toString();
            String savePath = Paths.get("treetracker-benchmark", "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "micro", "exp2p8", "Exp2P8Queries.java").toString();
            List<String> importStatements = new ArrayList<>();
            List<String> enumStatements = new ArrayList<>();
            List<String> cases = new ArrayList<>();
            for (GenerateGroup generateGroup : generateGroups) {
                enumStatements.addAll(generateEnumCodeBlock(generateGroup));
                cases.addAll(generateCaseCodeBlock(generateGroup));
            }
            buildEnumCodeBlock(context, enumStatements);
            buildCaseCodeBlock(context, cases);
            render(context, templatePath, savePath);
        }

        private static List<String> generateEnumCodeBlock(GenerateGroup generateGroup)
        {
            String codeBlock = "%s(\"%s\")";
            List<String> enumStatements = new ArrayList<>();
            for (String javaClass : generateGroup.benchmarkQueries) {
                enumStatements.add(String.format(codeBlock, javaClass, javaClass));
            }
            return enumStatements;
        }

        private static void buildEnumCodeBlock(Map<String, Object> context,
                                               List<String> enumStatements)
        {
            StringBuilder builder = new StringBuilder();
            builder.append(Joiner.on(",\n" + repeat(" ", 4)).join(enumStatements))
                    .append(";");
            context.put("EnumCodeBlock", builder.toString());
        }

        private static List<String> generateCaseCodeBlock(GenerateGroup generateGroup)
        {
            List<String> cases = new ArrayList<>();
            for (String javaClass : generateGroup.benchmarkQueries) {
                cases.add(buildACase(javaClass));
            }
            return cases;
        }

        private static void buildCaseCodeBlock(Map<String, Object> context,
                                               List<String> cases)
        {
            StringBuilder builder = new StringBuilder();
            builder.append(Joiner.on("\n" + repeat(" ", 16)).join(cases));
            context.put("caseCodeBlock", builder.toString());
        }

        private static String buildACase(String javaClass)
        {
            StringBuilder builder = new StringBuilder();
            builder.append("case ")
                    .append(javaClass)
                    .append(":\n");
            builder.append(repeat(" ", 20))
                    .append(String.format("map.put(c.val, %s.class);", javaClass))
                    .append("\n");
            builder.append(repeat(" ", 20))
                    .append("break;");
            return builder.toString();
        }
    }

    static class BenchmarkGen
    {
        public static void generateBenchmark(List<GenerateGroup> generateGroups)
                throws IOException
        {
            String templatePath = Paths.get("codegen", "exp2p8", "BenchmarkExp2P8.javat").toString();
            String savePath = Paths.get("treetracker-benchmark", "src", "main", "java", "org", "zhu45", "treetracker", "benchmark", "micro", "exp2p8", "BenchmarkExp2P8.java").toString();
            Map<String, Object> context = new HashMap<>();
            List<String> queries = new ArrayList<>();
            for (GenerateGroup generateGroup : generateGroups) {
                queries.addAll(generateExp2P8QueriesAnnotationBlock(generateGroup));
            }
            buildExp2P8QueriesAnnotationBlock(context, queries);
            render(context, templatePath, savePath);
        }

        private static List<String> generateExp2P8QueriesAnnotationBlock(GenerateGroup generateGroup)
        {
            List<String> queries = new ArrayList<>();
            for (String javaClass : generateGroup.benchmarkQueries) {
                queries.add("\"" + javaClass + "\"");
            }
            return queries;
        }

        private static void buildExp2P8QueriesAnnotationBlock(Map<String, Object> context,
                                                              List<String> queries)
        {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("@Param({");
            stringBuilder.append(Joiner.on(", ").join(queries))
                    .append("})");
            context.put("Exp2P8QueriesAnnotationBlock", stringBuilder.toString());
        }
    }

    public static void run(List<Integer> numberOfBackJumpedRelationList, int backjumpedRelationSize)
            throws IOException
    {
        List<GenerateGroup> generateGroups = new ArrayList<>();
        for (int numberOfBackJumpedRelations : numberOfBackJumpedRelationList) {
            generateGroups.add(new GenerateGroup(numberOfBackJumpedRelations, backjumpedRelationSize));
            generateData(generateGroups.get(generateGroups.size() - 1));
        }
        generateSQLScript(generateGroups);
        generateDatabase(generateGroups);
        generateQuery(generateGroups);
        generateQueryEnum(generateGroups);
        BenchmarkGen.generateBenchmark(generateGroups);
    }

    @Getter
    @Setter
    static class GenerateGroup
    {
        int numberOfBackJumpedRelations;
        int backjumpedRelationSize;
        List<String> relations;
        List<String> multiwayJoinNodes;
        List<String> benchmarkQueries = new ArrayList<>();
        List<String> csvFiles;
        Map<String, String> relation2CSVFileName;

        public GenerateGroup(int numberOfBackJumpedRelations, int backjumpedRelationSize)
        {
            this.numberOfBackJumpedRelations = numberOfBackJumpedRelations;
            this.backjumpedRelationSize = backjumpedRelationSize;
        }
    }

    public static void main(String[] args)
            throws IOException
    {
        List<Integer> numberOfBackJumpedRelationList = List.of(1, 2, 3, 4, 5, 6, 7, 8);
        run(numberOfBackJumpedRelationList, 10);
    }
}
