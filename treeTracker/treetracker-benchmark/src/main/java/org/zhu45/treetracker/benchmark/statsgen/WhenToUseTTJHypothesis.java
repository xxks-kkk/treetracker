package org.zhu45.treetracker.benchmark.statsgen;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.nio.file.Paths;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.statsgen.Benchmarks.SSB;
import static org.zhu45.treetracker.benchmark.statsgen.Benchmarks.TPCH;

/**
 * We check how powerful the following hypothesis is:
 * <p>
 * If cost of TTJ < cost of HJ + summation of inner relation size,
 * use TTJ instead of HJ, i.e., performance of TTJ is better than the
 * performance of HJ
 * <p>
 * False negative: cost of TTJ < cost of HJ but performance of HJ is better than the performance
 * of TTJ
 * (HJ is better than TTJ, i.e., negative, given the event cost of TTJ < cost of HJ present
 * per Wikipedia: false negative is the test result incorrectly indicates the absence of a condition when it is actually present)
 * False positive: cost of TTJ > cost of HJ but performance of TTJ is better than the performance of HJ
 * (TTJ is better than HJ, i.e., positive, given the event cost of TTJ < cost of HJ not present,
 * per Wikipedia: The false positive rate (FPR) is the proportion of all negatives that still yield positive test outcomes, i.e.,
 * the conditional probability of a positive test result given an event that was not present.)
 */
public class WhenToUseTTJHypothesis
{
    private WhenToUseTTJHypothesis()
    {
    }

    private static void run(Map<Benchmarks, String> benchmarkToCSV,
                            boolean useTrueStatistics,
                            boolean excludingFluctuationStats)
            throws IOException, ParseException
    {
        Map<Benchmarks, Map<String, Integer>> benchmarkToQueryToLabels = generateLabels(benchmarkToCSV, excludingFluctuationStats);
        Map<Benchmarks, List<String>> benchmarkToQueries = new HashMap<>();
        for (Benchmarks benchmark : benchmarkToQueryToLabels.keySet()) {
            benchmarkToQueries.put(benchmark, new ArrayList<>(benchmarkToQueryToLabels.get(benchmark).keySet()));
        }
        Map<Benchmarks, Map<String, DataFields>> hashJoinTrueStats = extractStats(benchmarkToQueries, JoinOperator.HASH_JOIN);
        Map<Benchmarks, Map<String, DataFields>> ttjTrueStats = extractStats(benchmarkToQueries, JoinOperator.TTJHP);
        computeResult(benchmarkToQueryToLabels, hashJoinTrueStats, ttjTrueStats, useTrueStatistics);
        saveCostEstimatesToCSV(hashJoinTrueStats, excludingFluctuationStats, useTrueStatistics, JoinOperator.HASH_JOIN);
        saveCostEstimatesToCSV(ttjTrueStats, excludingFluctuationStats, useTrueStatistics, JoinOperator.TTJHP);
    }

    private static void saveCostEstimatesToCSV(Map<Benchmarks, Map<String, DataFields>> stats,
                                               boolean excludingFluctuationStats,
                                               boolean useTrueStatistics,
                                               JoinOperator joinOperator)
    {
        for (Benchmarks benchmark : stats.keySet()) {
            String directoryPath = getDirectoryPath(false, benchmark);
            String fileExtension = useTrueStatistics ? "_costMeasurements.csv" : "_costEstimates.csv";
            String outputFileName = Paths.get(directoryPath,
                    joinOperator.name() + "_" + benchmark + "_" + excludingFluctuationStats + fileExtension).toString();
            List<String> headers = new ArrayList<>(List.of("statistics"));
            List<String> queries = new ArrayList<>(stats.get(benchmark).keySet());
            queries.sort((o1name, o2name) -> {
                String o1numStr = o1name.replaceAll("\\D", "");
                String o2numStr = o2name.replaceAll("\\D", "");
                int o1num = o1numStr.isEmpty() ? 0 : Integer.parseInt(o1numStr);
                int o2num = o2numStr.isEmpty() ? 0 : Integer.parseInt(o2numStr);
                if (o1num != o2num) {
                    return o1num - o2num;
                }
                return o1name.compareToIgnoreCase(o2name);
            });
            headers.addAll(queries);
            CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
            builder.setHeader(headers.toArray(new String[0]));
            try (Writer writer = new FileWriter(outputFileName, false);
                    CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
                List<Object> costEstimates = new ArrayList<>();
                costEstimates.add("cost");
                for (String query : queries) {
                    DataFields fields = stats.get(benchmark).get(query);
                    switch (joinOperator) {
                        case HASH_JOIN:
                            if (useTrueStatistics) {
                                costEstimates.add(fields.totalIntermediateResultsProducedWithoutNULL + fields.innerRelationSize);
                            }
                            else {
                                costEstimates.add(fields.costEstimates + fields.innerRelationSize);
                            }
                            break;
                        case TTJHP:
                            if (useTrueStatistics) {
                                costEstimates.add(fields.totalIntermediateResultsProducedWithoutNULL + fields.innerRelationSize - fields.totalTuplesRemovedInnerRelations);
                            }
                            else {
                                costEstimates.add(fields.costEstimates);
                            }
                            break;
                        default:
                            throw new RuntimeException();
                    }
                }
                csvPrinter.printRecord(costEstimates);
                System.out.println("result write to: " + outputFileName);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static void computeResult(Map<Benchmarks, Map<String, Integer>> benchmarkToQueryToLabels,
                                      Map<Benchmarks, Map<String, DataFields>> hashJoinBenchmarkToQueryToDataField,
                                      Map<Benchmarks, Map<String, DataFields>> ttjBenchmarkToQueryToDataField,
                                      boolean useTrueStatistics)
    {
        for (Benchmarks benchmark : benchmarkToQueryToLabels.keySet()) {
            System.out.println(benchmark + "stats: ");
            List<String> successPredicationQuery = new ArrayList<>();
            List<String> failedPredicationQuery = new ArrayList<>();
            List<String> falseNegativeQuery = new ArrayList<>();
            List<String> falsePositiveQuery = new ArrayList<>();
            Map<String, Integer> queryToLabels = benchmarkToQueryToLabels.get(benchmark);
            for (String query : queryToLabels.keySet()) {
                int label = queryToLabels.get(query);
                DataFields hashJoinDF = hashJoinBenchmarkToQueryToDataField.get(benchmark).get(query);
                DataFields ttjDF = ttjBenchmarkToQueryToDataField.get(benchmark).get(query);
                double hashJoinCost = 0;
                double ttjCost = 0;
                if (useTrueStatistics) {
                    hashJoinCost = hashJoinDF.innerRelationSize + hashJoinDF.totalIntermediateResultsProducedWithoutNULL;
                    ttjCost = ttjDF.totalIntermediateResultsProducedWithoutNULL + ttjDF.innerRelationSize - ttjDF.totalTuplesRemovedInnerRelations;
                }
                else {
                    hashJoinCost = hashJoinDF.costEstimates + hashJoinDF.innerRelationSize;
                    ttjCost = ttjDF.costEstimates;
                }
                if ((hashJoinCost <= ttjCost && label == 0) || (hashJoinCost > ttjCost && label == 1)) {
                    successPredicationQuery.add(query);
                }
                else {
                    failedPredicationQuery.add(query);
                    if (hashJoinCost > ttjCost && label == 0) {
                        falseNegativeQuery.add(query);
                    }
                    if (hashJoinCost < ttjCost && label == 1) {
                        falsePositiveQuery.add(query);
                    }
                }
                System.out.printf("query: %s\nhashJoinCost:%s\nttjCost:%s\nlabel:%s\n",
                        query, hashJoinCost, ttjCost, label);
            }
            System.out.printf("Success (%s) : %s\n", successPredicationQuery.size(), successPredicationQuery);
            System.out.printf("Failed (%s) : %s\n", failedPredicationQuery.size(), failedPredicationQuery);
            System.out.printf("False negative (%s): %s\n", falseNegativeQuery.size(), falseNegativeQuery);
            System.out.printf("False positive (%s): %s\n", falsePositiveQuery.size(), falsePositiveQuery);
        }
    }

    /**
     * We extract `totalIntermediateResultsProducedWithoutNULL`, `innerRelationSize`, and `totalTuplesRemovedInnerRelations`
     * from aggregate statistics jsons for given algorithm whenever those fields appear. And `cost` from plans.
     */
    private static Map<Benchmarks, Map<String, DataFields>> extractStats(Map<Benchmarks, List<String>> benchmarkToQueries,
                                                                         JoinOperator joinOperator)
            throws IOException, ParseException
    {
        Map<Benchmarks, Map<String, DataFields>> benchmarkToQueryToDataField = new HashMap<>();
        for (Benchmarks benchmark : benchmarkToQueries.keySet()) {
            benchmarkToQueryToDataField.put(benchmark, extractQueryToDataField(joinOperator,
                    benchmarkToQueries.get(benchmark), benchmark));
        }
        return benchmarkToQueryToDataField;
    }

    private static Map<String, DataFields> extractQueryToDataField(JoinOperator joinOperator,
                                                                   List<String> queries,
                                                                   Benchmarks benchmark)
            throws IOException, ParseException
    {
        Map<String, DataFields> queryToDataField = new HashMap<>();
        for (String query : queries) {
            String[] filePatterns = constructFilePattern(joinOperator, query, benchmark, true);
            File[] foundFiles = getAListOfTargetJsonFilesFromPattern(getDirectoryPath(true, benchmark), filePatterns);
            checkState(foundFiles.length == 1,
                    "foundFiles has size: " + foundFiles.length +
                            " with patterns: " + Arrays.toString(filePatterns) +
                            " they are: " + Arrays.toString(foundFiles));
            File foundFile = foundFiles[0];
            System.out.println("processing " + foundFile.getName() + " ...");
            String[] filePatterns2 = constructFilePattern(joinOperator, query, benchmark, false);
            File[] foundPlans = getAListOfTargetJsonFilesFromPattern(getDirectoryPath(false, benchmark), filePatterns2);
            checkState(foundPlans.length == 1,
                    "foundPlans has size: " + foundPlans.length +
                            " with patterns: " + Arrays.toString(filePatterns) +
                            " they are: " + Arrays.toString(foundPlans));
            File foundPlan = foundPlans[0];
            System.out.println("processing " + foundFile.getName() + " ...");
            queryToDataField.put(query, extractFields(query, foundFile, foundPlan));
        }
        return queryToDataField;
    }

    private static String[] constructFilePattern(JoinOperator joinOperator,
                                                 String query,
                                                 Benchmarks benchmark,
                                                 boolean useTrueStatistcs)
    {
        String extension = "";
        String benchmarkName = "";
        switch (benchmark) {
            case JOB:
                benchmarkName = "job";
        }
        switch (joinOperator) {
            case HASH_JOIN:
                if (useTrueStatistcs) {
                    extension = "FindOptJoinTree";
                }
                break;
            case TTJHP:
                if (useTrueStatistcs) {
                    extension = "OptJoinTreeOptOrdering";
                }
                else {
                    extension = "FindOptJoinTree";
                }
                break;
            default:
                throw new RuntimeException();
        }
        return List.of(joinOperator.name() + "_org", benchmarkName, extension, query).toArray(new String[0]);
    }

    private static DataFields extractFields(String query, File jsonFile, File jsonPlan)
            throws IOException, ParseException
    {
        return new DataFields(query, jsonFile.getAbsolutePath(), jsonPlan.getAbsolutePath());
    }

    private static class DataFields
    {
        private final String queryName;
        private final double totalIntermediateResultsProducedWithoutNULL;
        private final double innerRelationSize;
        private final double totalTuplesRemovedInnerRelations;
        private final double costEstimates;

        public DataFields(String query,
                          String file,
                          String plan)
                throws IOException, ParseException
        {
            this.queryName = query;
            Object obj = new JSONParser().parse(new FileReader(file));
            JSONObject jo = (JSONObject) obj;
            JSONObject aggregateStats = (JSONObject) jo.get("Aggregation Stats");
            totalIntermediateResultsProducedWithoutNULL = Double.parseDouble(aggregateStats.get("totalIntermediateResultsProducedWithoutNULL").toString());
            innerRelationSize = Double.parseDouble(aggregateStats.get("innerRelationSize").toString());
            Object totalTuplesRemovedInnerRelationsObj = aggregateStats.get("totalTuplesRemovedInnerRelations");
            if (totalTuplesRemovedInnerRelationsObj != null) {
                totalTuplesRemovedInnerRelations = Double.parseDouble(totalTuplesRemovedInnerRelationsObj.toString());
            }
            else {
                totalTuplesRemovedInnerRelations = 0;
            }
            Object obj2 = new JSONParser().parse(new FileReader(plan));
            JSONObject jo2 = (JSONObject) obj2;
            costEstimates = Double.parseDouble(jo2.get("cost").toString());
        }
    }

    private static String getDirectoryPath(boolean useTrueStatistics, Benchmarks benchmark)
    {
        if (useTrueStatistics) {
            return SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
        }
        switch (benchmark) {
            case JOB:
                return JOB_WITH_PREDICATES_RESULT_STORED_PATH;
            case SSB:
                return SSB_RESULT_STORED_PATH;
            case TPCH:
                return TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
            default:
                throw new RuntimeException();
        }
    }

    private static File[] getAListOfTargetJsonFilesFromPattern(String directoryPath, String[] pattern)
    {
        return new File(directoryPath).listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                return Stream.of(pattern).allMatch(name::contains) && name.endsWith(".json");
            }
        });
    }

    /**
     * Generate labels (0 or 1) based on performance aggregation CSVs. 1 means TTJ outperforms HJ and 0
     * means otherwise.
     */
    private static Map<Benchmarks, Map<String, Integer>> generateLabels(Map<Benchmarks, String> benchmarkToCSV,
                                                                        boolean excludingFluctuationStats)
            throws IOException
    {
        Map<Benchmarks, String> benchmarkToCSVFullPath = constructPaths(benchmarkToCSV);
        Map<Benchmarks, Map<String, Integer>> benchmarkToQueryToLabels = new HashMap<>();
        for (Benchmarks benchmark : benchmarkToCSVFullPath.keySet()) {
            HashMap<String, Integer> queryToLabels = new HashMap<>();
            String csvFullPath = benchmarkToCSVFullPath.get(benchmark);
            Map<String, Double> performanceImprovementPercentage = extractPerformanceImprovementPercentageFromCSV(csvFullPath);
            for (String query : performanceImprovementPercentage.keySet()) {
                if (!excludingFluctuationStats || (performanceImprovementPercentage.get(query) > 0.05 ||
                        performanceImprovementPercentage.get(query) < -0.05)) {
                    if (performanceImprovementPercentage.get(query) >= 0) {
                        queryToLabels.put(query, 1);
                    }
                    else {
                        queryToLabels.put(query, 0);
                    }
                }
                else {
                    System.out.printf("Excluding query: %s because its performance is within -5%% ~ 5%%: %s\n",
                            query, performanceImprovementPercentage.get(query));
                }
            }
            benchmarkToQueryToLabels.put(benchmark, queryToLabels);
        }
        return benchmarkToQueryToLabels;
    }

    private static Map<String, Double> extractPerformanceImprovementPercentageFromCSV(String csvPath)
            throws IOException
    {
        Map<String, Double> performanceImprovementPercentage = new HashMap<>();
        try (Reader csv = new FileReader(csvPath)) {
            CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                    .setHeader()
                    .setSkipHeaderRecord(false)
                    .build();
            Iterable<CSVRecord> records = csvFormat.parse(csv);
            Iterator<CSVRecord> recordIterator = records.iterator();
            List<String> headers = new ArrayList<>(recordIterator.next().toMap().keySet());
            while (recordIterator.hasNext()) {
                CSVRecord record = recordIterator.next();
                if (record.get("Algorithm").equals("TTJHP perf improv over HJ")) {
                    headers.remove(0);
                    for (String query : headers) {
                        performanceImprovementPercentage.put(query, Double.valueOf(record.get(query)));
                    }
                    return performanceImprovementPercentage;
                }
            }
        }
        throw new RemoteException("target row `TTJHP perf improv over HJ` not found");
    }

    private static Map<Benchmarks, String> constructPaths(Map<Benchmarks, String> benchmarkToCSV)
    {
        Map<Benchmarks, String> benchmarkToCSVFullPath = new HashMap<>();
        for (Benchmarks benchmark : benchmarkToCSV.keySet()) {
            switch (benchmark) {
                case JOB:
                    benchmarkToCSVFullPath.put(Benchmarks.JOB,
                            Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, benchmarkToCSV.get(benchmark)).toString());
                    break;
                case TPCH:
                    benchmarkToCSVFullPath.put(TPCH,
                            Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, benchmarkToCSV.get(benchmark)).toString());
                    break;
                case SSB:
                    benchmarkToCSVFullPath.put(SSB,
                            Paths.get(SSB_RESULT_STORED_PATH, benchmarkToCSV.get(benchmark)).toString());
                    break;
            }
        }
        return benchmarkToCSVFullPath;
    }

    public static void main(String[] args)
            throws IOException, ParseException
    {
        run(Map.of(Benchmarks.JOB, "2023-08-07T04:40:13.605620Z_perf_report.csv"),
                true, true);
    }
}
