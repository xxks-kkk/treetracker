package org.zhu45.treetracker.benchmark.statsgen;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;

/**
 * Generate optimal join ordering summary csv for HASH JOIN
 */
public class GenerateOptimalJoinOrderingCSV
{
    private GenerateOptimalJoinOrderingCSV()
    {
    }

    public static void generateOptimalJoinOrderingCSV(String planStatisticsPath, Map<String, String> relation2MultiwayJoinNode)
            throws IOException, ParseException
    {
        // TODO: sharing much code with GenerateJOBBenchmarkWithOptimalJoinTrees. Probably
        // can refactor.
        File[] files = getAListOfTargetJsonFiles(planStatisticsPath);

        List<DataFields> dataFields = new ArrayList<>();
        int maxRelationSize = 0;
        for (File jsonFile : files) {
            System.out.println("processing " + jsonFile.getName() + " ...");
            DataFields jsonFields = extractFields(jsonFile, relation2MultiwayJoinNode);
            dataFields.add(jsonFields);
            if (jsonFields.relationNames.size() > maxRelationSize) {
                maxRelationSize = jsonFields.relationNames.size();
            }
        }
        dataFields.sort(Comparator.comparing(DataFields::getQueryName));

        String outputFileName = Paths.get(planStatisticsPath, "OptimalJoinOrderingHashJoin.csv").toString();
        String[] headers = new String[maxRelationSize + 1];
        headers[0] = "Query";
        for (int i = 0; i < maxRelationSize; ++i) {
            headers[i + 1] = "Relation" + i;
        }
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        builder.setHeader(headers);
        try (Writer writer = new FileWriter(outputFileName, false);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            for (DataFields dataFields1 : dataFields) {
                List<String> data = new ArrayList<>();
                data.add(dataFields1.queryName);
                data.addAll(dataFields1.relationNames);
                csvPrinter.printRecord(data);
            }
            System.out.println("result write to: " + outputFileName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File[] getAListOfTargetJsonFiles(String planStatisticsPath)
    {
        String[] patterns = "HASH_JOIN,planStatistics".split(",");
        return new File(planStatisticsPath).listFiles((dir, name) -> Stream.of(patterns).allMatch(name::contains) && name.endsWith(".json"));
    }

    public static DataFields extractFields(File jsonFile, Map<String, String> relation2MultiwayJoinNode)
            throws IOException, ParseException
    {
        return new DataFields(jsonFile.getAbsolutePath(), relation2MultiwayJoinNode);
    }

    public static class DataFields
    {
        public final List<String> relationNames;
        public final String queryName;

        public DataFields(String file, Map<String, String> relation2MultiwayJoinNode)
                throws IOException, ParseException
        {
            Object obj = new JSONParser().parse(new FileReader(file));
            JSONObject jo = (JSONObject) obj;
            relationNames = setRelationNames((JSONArray) ((JSONObject) jo.get("optimalJoinOrdering")).get("schemaTableNameList"),
                    relation2MultiwayJoinNode);
            queryName = setQueryName(file);
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
         * Assuming "HASH_JOIN_org.zhu45.treetracker.benchmark.job.q1.Query1a_planStatistics.json"
         */
        public static String setQueryName(String file)
        {
            String[] tokens = file.split("\\.");
            String target = tokens[tokens.length - 2];
            int endIndex = target.indexOf("_planStatistics");
            return target.substring(0, endIndex);
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

        public String getQueryName()
        {
            return queryName;
        }
    }
}
