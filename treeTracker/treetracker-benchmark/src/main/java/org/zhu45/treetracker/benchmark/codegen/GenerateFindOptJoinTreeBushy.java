package org.zhu45.treetracker.benchmark.codegen;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zhu45.treetracker.benchmark.Benchmarks;
import org.zhu45.treetracker.benchmark.ssb.SSBDatabase;
import org.zhu45.treetracker.benchmark.statsgen.GenerateOptimalJoinOrderingCSV;
import org.zhu45.treetracker.benchmark.tpch.TPCHDatabase;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.relational.planner.PostgresPlanProperty;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_UPDATED_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_UPDATED_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_UPDATED_POSTGRES_PLAN_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.codegen.GenerateFindOptJoinTree.Step1.generateJavaClass;
import static org.zhu45.treetracker.benchmark.codegen.GenerateFindOptJoinTree.Step2.step2Driver;
import static org.zhu45.treetracker.benchmark.codegen.GenerateFindOptJoinTreeBushy.Step1.step1Driver;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2SavePath;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.queryName2Template;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.relation2MultiwayJoinNode;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.savePathPrefix;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.CodeGen.templatePathPrefix;
import static org.zhu45.treetracker.benchmark.statsgen.GenerateOptimalJoinOrderingCSV.DataFields.getCanonicalRelation;
import static org.zhu45.treetracker.benchmark.statsgen.GenerateOptimalJoinOrderingCSV.extractFields;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.common.StandardErrorCode.INVALID_USAGE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.ALIAS;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.NODE_TYPE;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.PLAN;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.PLANS;
import static org.zhu45.treetracker.relational.planner.PostgresPlanProperty.RELATION_NAME;
import static org.zhu45.treetracker.relational.planner.testing.postgresplan.PostgresPlanGenerator.generatePostgresPlanJson;

/**
 * Purpose:
 * Takes in Postrges plans and produces corresponding *FindOptJoinTree.java for HJ, which can be used for benchmarking HJ performance
 * or to find the best join ordering & join tree, or join tree for the fixed ordering for TTJ. We illustrate steps using JOB as an example.
 * <p>
 * Steps:
 * 1. The program finds all the Postgres plans and for each Postgres plan, the program creates *FindOptJoinTree.java.
 * 2. After creating all the *FindOptJoinTree.java, the program creates JOBQueriesFindOptJoinTree.java to register
 * those classes to be used in the benchmark.
 */
public class GenerateFindOptJoinTreeBushy
{
    private GenerateFindOptJoinTreeBushy()
    {
    }

    public static class Step1
    {
        /**
         * Generate all the *FindOptJoinTree.java from Postgres plans jsons
         */
        static List<String> step1Driver(String postgresPlanPath,
                                        String postgresPlanUpdatedPath,
                                        String planStatisticsPath,
                                        Map<String, String> relation2MultiwayJoinNode,
                                        String templatePathPrefix,
                                        Map<String, String> queryName2Template,
                                        String savePathPrefix,
                                        Map<String, String> queryName2SavePath)
                throws IOException, ParseException
        {
            File[] planStatisticsFiles = GenerateFindOptJoinTree.Step1.getAListOfTargetJsonFiles(planStatisticsPath);
            File[] postgresPlanFiles = getAListOfTargetJsonFiles(postgresPlanPath);
            List<String> queryNames = new ArrayList<>();
            Map<File, File> postgresPlanFileToPlanStatisticsFile = mapPostgresPlanFileWithPlanStatisticFile(postgresPlanFiles, planStatisticsFiles);
            for (Map.Entry<File, File> jsonFiles : postgresPlanFileToPlanStatisticsFile.entrySet()) {
                File postgresJsonFile = jsonFiles.getKey();
                File planStatisticsFile = jsonFiles.getValue();
                System.out.println("processing " + postgresJsonFile.getName() + " ...");
                System.out.println("matched with " + planStatisticsFile.getName() + " ...");
                GenerateOptimalJoinOrderingCSV.DataFields jsonFields = extractFields(planStatisticsFile, relation2MultiwayJoinNode);
                List<String> desiredRelationNameList = extractRelationNamesFromPlanStatistics(planStatisticsFile);
                String updatedPostgresPlanPath = modifyRelationNamesInPostgresPlan(postgresPlanUpdatedPath, postgresJsonFile, desiredRelationNameList, relation2MultiwayJoinNode);
                createFindOptJoinTreeClass(postgresPlanUpdatedPath,
                        postgresJsonFile,
                        jsonFields.relationNames,
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

        private static Map<File, File> mapPostgresPlanFileWithPlanStatisticFile(File[] postgresPlanFiles, File[] planStatisticsFiles)
        {
            requireNonNull(postgresPlanFiles);
            Map<File, File> postgresPlanFileToPlanStatisticsFile = new HashMap<>();
            for (File postgresPlanFile : postgresPlanFiles) {
                String postgresPlanFileQueryName = setQueryName(postgresPlanFile.getName());
                for (File planStatisticsFile : planStatisticsFiles) {
                    String planStatisticFileQueryName = GenerateOptimalJoinOrderingCSV.DataFields.setQueryName(planStatisticsFile.getName());
                    if (postgresPlanFileQueryName.equals(planStatisticFileQueryName)) {
                        postgresPlanFileToPlanStatisticsFile.put(postgresPlanFile, planStatisticsFile);
                    }
                }
            }
            return postgresPlanFileToPlanStatisticsFile;
        }

        private static List<String> extractRelationNamesFromPlanStatistics(File planStatisticsFile)
                throws IOException, ParseException
        {
            Object obj = new JSONParser().parse(new FileReader(planStatisticsFile));
            JSONObject jo = (JSONObject) obj;
            JSONArray schemaTableNameList = (JSONArray) ((JSONObject) jo.get("optimalJoinOrdering")).get("schemaTableNameList");
            List<String> relations = new ArrayList<>();
            for (Object object : schemaTableNameList) {
                JSONObject jsonObject = (JSONObject) object;
                relations.add((String) jsonObject.get("tableName"));
            }
            return relations;
        }

        private static String getPostgrePlanFromJson(File jsonFile)
                throws IOException
        {
            return Files.readString(jsonFile.toPath());
        }

        /**
         * Assuming "1a.json" and outputs "Query1a"
         */
        private static String setQueryName(String file)
        {
            String[] tokens = file.split("\\.");
            String target = tokens[0];
            return "Query" + target;
        }

        private static File[] getAListOfTargetJsonFiles(String planStatisticsPath)
        {
            return new File(planStatisticsPath).listFiles((dir, name) -> name.endsWith(".json"));
        }

        private static String obtainUpdatedPostgresPlanName(File postgresJsonFile)
        {
            return "updated_" + postgresJsonFile.getName();
        }

        private static Path obtainUpdatedPostgresPlanPath(File postgresJsonFile, String postgresPlanUpdatedPath)
        {
            return Paths.get(postgresPlanUpdatedPath, obtainUpdatedPostgresPlanName(postgresJsonFile));
        }

        private static String modifyRelationNamesInPostgresPlan(String postgresPlanUpdatedPath, File postgresJsonFile, List<String> desiredRelationNameList, Map<String, String> relation2MultiwayJoinNode)
        {
            try {
                String postgresPlan = getPostgrePlanFromJson(postgresJsonFile);
                Object obj = new JSONParser().parse(postgresPlan);
                JSONArray jo = (JSONArray) obj;
                modifyRelationNamesInPostgresPlanHelper((JSONObject) ((JSONObject) jo.get(0)).get(PLAN.getValue()), desiredRelationNameList, relation2MultiwayJoinNode);
                ObjectMapper objectMapper = new ObjectMapper();
                Path outputPath = obtainUpdatedPostgresPlanPath(postgresJsonFile, postgresPlanUpdatedPath);
                objectMapper.writeValue(outputPath.toFile(), jo);
                return outputPath.toString();
            }
            catch (ParseException e) {
                throw new TreeTrackerException(INVALID_USAGE, "postgresPlan cannot be parsed as JSON\n" + e);
            }
            catch (IOException e) {
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, "Error encountered when updating Postgres plan with updated relation names: \n" + e);
            }
        }

        /**
         * Since the Postgres plans we use are from the original JOB queries but our query implementation leverages views, there is a gap
         * in terms of relation names between those in Postgres plans and those in query implementation. The solution is to
         * close the gap by modifying Postgres plan relation names based on the relation names from query implementation.
         */
        private static void modifyRelationNamesInPostgresPlanHelper(JSONObject planRoot, List<String> desiredRelationNameList, Map<String, String> relation2MultiwayJoinNode)
        {
            JSONArray plans;
            switch (PostgresPlanProperty.getNodeType((String) planRoot.get(PostgresPlanProperty.NODE_TYPE.getValue()))) {
                case HASH:
                case GATHER:
                case SORT:
                case AGGREGATE:
                case MATERIALIZE:
                case GATHER_MERGE:
                    plans = (JSONArray) planRoot.get(PLANS.getValue());
                    checkState(plans.size() == 1, planRoot.get(NODE_TYPE.getValue()) + " NodeType has more than one plans");
                    modifyRelationNamesInPostgresPlanHelper((JSONObject) plans.get(0), desiredRelationNameList, relation2MultiwayJoinNode);
                    break;
                case SEQ_SCAN:
                    checkState(planRoot.containsKey(RELATION_NAME.getValue()), "We run into an unexpected node: " + planRoot);
                    planRoot.put(RELATION_NAME.getValue(), determineRelationNameToReplace(planRoot, desiredRelationNameList, relation2MultiwayJoinNode));
                    break;
                case HASH_JOIN:
                case NESTED_LOOP:
                case MERGE_JOIN:
                    plans = (JSONArray) planRoot.get(PLANS.getValue());
                    checkState(plans.size() == 2, "Join NodeType doesn't have exactly two plans");
                    modifyRelationNamesInPostgresPlanHelper((JSONObject) plans.get(0), desiredRelationNameList, relation2MultiwayJoinNode);
                    modifyRelationNamesInPostgresPlanHelper((JSONObject) plans.get(1), desiredRelationNameList, relation2MultiwayJoinNode);
                    break;
                default:
                    throw new TreeTrackerException(GENERIC_INTERNAL_ERROR,
                            "Unexpected NodeType: " + planRoot.get(NODE_TYPE.getValue()));
            }
        }

        private static String determineRelationNameToReplace(JSONObject planRoot, List<String> desiredRelationNameList, Map<String, String> relation2MultiwayJoinNode)
        {
            String relationInPlan = (String) planRoot.get(RELATION_NAME.getValue());
            List<String> candidateRelationNames = new ArrayList<>();
            for (String desiredRelation : desiredRelationNameList) {
                String possibleCandidateRelation = getCanonicalRelation(desiredRelation, relation2MultiwayJoinNode).replaceAll("\\d", "");
                if (relationInPlan.equals(possibleCandidateRelation)) {
                    candidateRelationNames.add(desiredRelation);
                }
            }
            // In a given benchmark query, there can be multiple base relation but with different aliases. We cannot directly replace
            // relation name in Postgres plan with the direct match. We have to consider alias.
            if (candidateRelationNames.size() == 1) {
                return candidateRelationNames.get(0);
            }
            else if (candidateRelationNames.size() > 1) {
                checkState(planRoot.containsKey(ALIAS.getValue()), ALIAS.getValue() + " field doesn't appear in the given node: " + planRoot);
                String alias = (String) planRoot.get(ALIAS.getValue());
                List<Long> desiredRelationIndices = candidateRelationNames.stream().map(c -> {
                    try {
                        List<String> parts = Arrays.asList(c.split("_"));
                        String withoutPrefix = Joiner.on("").join(parts.subList(1, parts.size()));
                        return Long.parseLong(StringUtils.getDigits(withoutPrefix));
                    }
                    catch (NumberFormatException e) {
                        return 0L;
                    }
                }).collect(Collectors.toList());
                String aliasIdx = StringUtils.getDigits(alias);
                if (!aliasIdx.equals("")) {
                    long idx = Long.parseLong(StringUtils.getDigits(alias));
                    return candidateRelationNames.stream().filter(c -> c.contains(relationInPlan + idx)).findFirst().orElseThrow(
                            () -> new TreeTrackerException(GENERIC_INTERNAL_ERROR, relationInPlan + idx + " is not contained in " + desiredRelationNameList));
                }
                else {
                    // We handle the case that in our query implementation, we have R and R1 where "R" has no digits in it will be mapped to
                    // 0L in desiredRelationIndices.
                    return candidateRelationNames.stream().filter(c -> c.contains(relationInPlan)).findFirst().orElseThrow(
                            () -> new TreeTrackerException(GENERIC_INTERNAL_ERROR, "expect " + desiredRelationNameList + " to contain " + relationInPlan));
                }
            }
            else {
                throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, String.format("Relation name %s from Postgres plan is not found in %s",
                        planRoot.get(RELATION_NAME.getValue()), desiredRelationNameList));
            }
        }

        private static void createFindOptJoinTreeClass(
                String postgresPlanUpdatedPath,
                File postgresJsonFile,
                List<String> relationNames,
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
            context.put("genPostgresPlan", "1");
            generateCreatePhysicalPlanFromJoinOrderingCodeBlock(context, postgresPlanUpdatedPath, relationNames, relation2MultiwayJoinNode, postgresJsonFile);
            generateJavaClass(context, queryName, templatePathPrefix, queryName2Template, savePathPrefix, queryName2SavePath);
        }

        /***
         * Get the name of postgresPlanUpdatedPath. If postgresPlanUpdatedPath = "a/b/c", this method returns "postgresPlanUpdatedPath".
         */
        private static String getVariable(String postgresPlanUpdatedPath)
        {
            try {
                for (Field f : Benchmarks.class.getDeclaredFields()) {
                    if (f.get(null).equals(postgresPlanUpdatedPath)) {
                        return f.getName();
                    }
                }
            }
            catch (IllegalAccessException e) {
                throw new TreeTrackerException(INVALID_USAGE, postgresPlanUpdatedPath + " is not found as a value of a field of Benchmarks class\n" + e);
            }
            throw new TreeTrackerException(INVALID_USAGE, postgresPlanUpdatedPath + " is not found as a value of a field of Benchmarks class");
        }

        private static void generateCreatePhysicalPlanFromJoinOrderingCodeBlock(Map<String, Object> context,
                                                                                String postgresPlanUpdatedPath,
                                                                                List<String> relationNames,
                                                                                Map<String, String> relation2MultiwayJoinNode,
                                                                                File postgresJsonFile)
        {
            StringBuilder stringBuilder = new StringBuilder();
            List<String> multiwayJoinNodes = relationNames.stream()
                    .map(relation2MultiwayJoinNode::get).map(n -> n + ".getSchemaTableName()").collect(Collectors.toList());
            stringBuilder.append("Pair<Plan, List<Operator>> pair = createPhysicalPlanFromPostgresPlan(")
                    .append(String.format("Paths.get(%s, ", getVariable(postgresPlanUpdatedPath)))
                    .append("\"")
                    .append(obtainUpdatedPostgresPlanName(postgresJsonFile))
                    .append("\").toString()")
                    .append(", ")
                    .append("List.of(")
                    .append(Joiner.on(", ").join(multiwayJoinNodes))
                    .append("));\n");
            context.put("createPhysicalPlanFromJoinOrderingCodeBlock", stringBuilder.toString());
        }
    }

    private static String getJavaClassName(String queryName)
    {
        return queryName + "FindOptJoinTree";
    }

    private static void run(String postgresPlanPath,
                            String postgresPlanUpdatedPath,
                            String planStatisticsPath,
                            Map<String, String> relation2MultiwayJoinNode,
                            String templatePathPrefix,
                            Map<String, String> queryName2Template,
                            String savePathPrefix,
                            Map<String, String> queryName2SavePath,
                            String importPrefix,
                            String queriesFindOptJoinTreeTemplatePath)
            throws IOException, ParseException
    {
        List<String> queryNames = step1Driver(postgresPlanPath, postgresPlanUpdatedPath, planStatisticsPath, relation2MultiwayJoinNode,
                templatePathPrefix, queryName2Template, savePathPrefix, queryName2SavePath);
        step2Driver(queryNames, queryName2SavePath, importPrefix, savePathPrefix, queriesFindOptJoinTreeTemplatePath);
    }

    public static void generateFindOptJoinTreeJOBPostgres()
            throws IOException, ParseException
    {
        run(JOB_WITH_PREDICATES_RESULT_POSTGRES_PLAN_STORED_PATH,
                JOB_WITH_PREDICATES_RESULT_UPDATED_POSTGRES_PLAN_STORED_PATH,
                JOB_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH,
                relation2MultiwayJoinNode,
                templatePathPrefix,
                queryName2Template,
                savePathPrefix,
                queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.job",
                "codegen/job/JOBQueriesFindOptJoinTree.javat");
    }

    public static void generateFindOptJoinTreeSSBPostgres()
            throws IOException, ParseException
    {
        run(SSB_POSTGRES_PLAN_STORED_PATH,
                SSB_UPDATED_POSTGRES_PLAN_STORED_PATH,
                SSB_SQLITE_ORDERING_STORED_PATH,
                SSBDatabase.CodeGen.relation2MultiwayJoinNode,
                SSBDatabase.CodeGen.templatePathPrefix,
                SSBDatabase.CodeGen.queryName2Template,
                SSBDatabase.CodeGen.savePathPrefix,
                SSBDatabase.CodeGen.queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.ssb",
                "codegen/ssb/SSBQueriesFindOptJoinTree.javat");
    }

    /**
     * Plans hardcoded here are based on original Postgres plans of 15W, 18W, and 20W. We do this to
     * simplify our query generation for these two benchmark queries.
     */
    private static void saveTPCH15W18W20WManually()
    {
        String postgresPlan15W = generatePostgresPlanJson("AGG,Outer\n" +
                "|JOIN,Outer\n" +
                "||TAB,supplier,Outer\n" +
                "||HASH,Inner\n" +
                "|||TAB,lineitem,Outer");
        try (PrintWriter out = new PrintWriter(Paths.get(TPCH_POSTGRES_PLAN_STORED_PATH, "15W.json").toString())) {
            out.println(postgresPlan15W);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        String postgresPlan20W = generatePostgresPlanJson("JOIN,Outer\n" +
                "|TAB,supplier,Outer\n" +
                "|HASH,Inner\n" +
                "||TAB,nation,Outer");
        try (PrintWriter out = new PrintWriter(Paths.get(TPCH_POSTGRES_PLAN_STORED_PATH, "20W.json").toString())) {
            out.println(postgresPlan20W);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        String postgresPlan18W = generatePostgresPlanJson("JOIN,Outer\n" +
                "|TAB,lineitem,Outer\n" +
                "|HASH,Inner\n" +
                "||JOIN,Outer\n" +
                "|||TAB,orders,Outer\n" +
                "|||HASH,Inner\n" +
                "||||TAB,customer,Outer");
        try (PrintWriter out = new PrintWriter(Paths.get(TPCH_POSTGRES_PLAN_STORED_PATH, "18W.json").toString())) {
            out.println(postgresPlan18W);
        }
        catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static void generateFindOptJoinTreeTPCHPostgres()
            throws IOException, ParseException
    {
        saveTPCH15W18W20WManually();
        run(TPCH_POSTGRES_PLAN_STORED_PATH,
                TPCH_UPDATED_POSTGRES_PLAN_STORED_PATH,
                TPCH_WITH_PREDICATES_RESULT_SQLITE_ORDERING_STORED_PATH,
                TPCHDatabase.CodeGen.relation2MultiwayJoinNode,
                TPCHDatabase.CodeGen.templatePathPrefix,
                TPCHDatabase.CodeGen.queryName2Template,
                TPCHDatabase.CodeGen.savePathPrefix,
                TPCHDatabase.CodeGen.queryName2SavePath,
                "import org.zhu45.treetracker.benchmark.tpch",
                "codegen/tpch/TPCHQueriesFindOptJoinTree.javat");
    }

    public static void main(String[] args)
            throws IOException, ParseException
    {
//        generateFindOptJoinTreeJOBPostgres();
//        generateFindOptJoinTreeSSBPostgres();
        generateFindOptJoinTreeTPCHPostgres();
    }
}
