package org.zhu45.treetracker.benchmark.statsgen;

import com.google.common.base.Joiner;
import de.renebergelt.test.Switches;
import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;
import lombok.Getter;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.json.simple.JSONArray;
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
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.zhu45.treetracker.benchmark.Benchmarks.JOB_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SAME_ORDERING_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_TRUE_CARD_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.SSB_RESULT_STORED_PATH;
import static org.zhu45.treetracker.benchmark.Benchmarks.TPCH_WITH_PREDICATES_RESULT_STORED_PATH;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.relational.operator.JoinOperator.HASH_JOIN;
import static org.zhu45.treetracker.relational.operator.JoinOperator.LIP;
import static org.zhu45.treetracker.relational.operator.JoinOperator.TTJHP;
import static org.zhu45.treetracker.relational.operator.JoinOperator.TTJHP_NO_NG;
import static org.zhu45.treetracker.relational.operator.JoinOperator.Yannakakis;
import static org.zhu45.treetracker.relational.operator.JoinOperator.YannakakisB;

/**
 * We generate statistics report that consists of two part:
 * 1. Overall performance of the whole benchmark
 * 2. Statistics based on aggregate statistics collected
 */
public class GenerateBenchmarkStatisticsReport
{
    private static final Logger traceLogger;

    // If data is missing, we assume it's due to time out, which is set 20 minutes.
    private static final double missingDataTimeOut = 1.2e+6;

    static {
        if (Switches.DEBUG) {
            traceLogger = LogManager.getLogger(GenerateBenchmarkStatisticsReport.class.getName());
        }
        else {
            traceLogger = null;
        }
    }

    private GenerateBenchmarkStatisticsReport()
    {
    }

    private static File[] getAListOfTargetJsonFiles(String planStatisticsPath,
                                                    Benchmarks benchmark,
                                                    JoinOperator joinOperator,
                                                    TargetStatsConstraints targetStatsConstraints)
    {
        List<String> patterns;
        if (benchmark != Benchmarks.SSB) {
            patterns = new ArrayList<>(List.of("OptJoinTreeOptOrdering"));
        }
        else {
            patterns = new ArrayList<>();
        }
        switch (joinOperator) {
            case TTJHP:
                patterns.add("TTJHP_org");
                break;
            case HASH_JOIN:
                if (targetStatsConstraints.hashJoinOnTTJOrdering && benchmark != Benchmarks.SSB) {
                    patterns = new ArrayList<>(List.of("OptJoinTreeOptOrdering"));
                }
                else if (targetStatsConstraints.hashJoinOnYAOrdering && benchmark != Benchmarks.SSB) {
                    patterns = new ArrayList<>(List.of("HJOnY"));
                }
                else if (!targetStatsConstraints.sameOrdering && benchmark != Benchmarks.SSB) {
                    patterns = new ArrayList<>(List.of("FindOptJoinTree"));
                }
                patterns.add("HASH_JOIN");
                break;
            case TTJHP_NO_NG:
                patterns.add("TTJHP_NO_NG");
                break;
            case LIP:
                patterns.add("LIP");
                break;
            case Yannakakis:
                patterns.add("Yannakakis");
                break;
            case TTJHP_BF:
                patterns.add("TTJHP_BF");
            case TTJHP_BG:
                patterns.add("TTJHP_BG");
            case YannakakisB:
                patterns.add("YannakakisB");
                break;
            default:
                throw new RuntimeException("Unsupported joinOperator: " + joinOperator);
        }
        switch (benchmark) {
            case JOB:
                patterns.add("job");
                break;
            case SSB:
                patterns.add("ssb");
                break;
            case TPCH:
                patterns.add("tpch");
                break;
            default:
                throw new RuntimeException();
        }
        List<String> finalPatterns = patterns;
        return new File(planStatisticsPath).listFiles(new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String name)
            {
                if (targetStatsConstraints.hashJoinOnTTJOrdering) {
                    return Stream.of(finalPatterns.toArray(new String[0]))
                            .allMatch(name::contains) &&
                            name.endsWith(".json") &&
                            !name.contains("HJOnY");
                }
                else if (!finalPatterns.contains("Yannakakis")) {
                    return Stream.of(finalPatterns.toArray(new String[0]))
                            .allMatch(name::contains) && name.endsWith(".json");
                }
                else {
                    return Stream.of(finalPatterns.toArray(new String[0]))
                            .allMatch(name::contains) &&
                            name.endsWith(".json") &&
                            !name.contains("YannakakisB");
                }
            }
        });
    }

    public static CSVMeta generateAggregateStatisticsReport(JoinOperator joinOperator,
                                                            Benchmarks benchmark,
                                                            TargetStatsConstraints targetStatsConstraints)
            throws IOException, ParseException
    {
        return generateAggregateStatisticsReport(decideAggStatsPath(targetStatsConstraints.sameOrdering, targetStatsConstraints.useTrueCard),
                benchmark,
                joinOperator,
                targetStatsConstraints);
    }

    public static void generatePerformanceReport(List<String> pathToPerformanceRunJsons,
                                                 Benchmarks benchmarks,
                                                 boolean showPerfNumber,
                                                 boolean enableFillMissingData)
            throws IOException, ParseException
    {
        HashMap<String, HashMap<String, Double>> query2JoinOperator2Perf = new HashMap<>();
        Set<String> appearedJoinOperator = new HashSet<>();
        boolean hashJoinIncluded = false;
        for (String pathToPerformanceRunJson : pathToPerformanceRunJsons) {
            HashMap<String, HashMap<String, Double>> query2JoinOperator2PerfTmp = new HashMap<>();
            Object obj = new JSONParser().parse(new FileReader(pathToPerformanceRunJson));
            JSONArray jo = (JSONArray) obj;
            for (Object object : jo) {
                JSONObject group = (JSONObject) object;
                String rawQuery = getRawQuery(benchmarks, group);
                String query = getCanonicalQuery(rawQuery);
                query2JoinOperator2PerfTmp.computeIfAbsent(query, k -> new HashMap<>());
                String joinOperator = (String) ((JSONObject) group.get("params")).get("joinOperator");
                if (joinOperator.equals(HASH_JOIN.name())) {
                    hashJoinIncluded = true;
                }
                appearedJoinOperator.add(joinOperator);
                Double perf = (Double) ((JSONObject) group.get("primaryMetric")).get("score");
                query2JoinOperator2PerfTmp.get(query).put(joinOperator, perf);
            }
            int numJoinOperators = appearedJoinOperator.size();
            for (String query : query2JoinOperator2PerfTmp.keySet()) {
                HashMap<String, Double> dp = query2JoinOperator2PerfTmp.get(query);
                if (!query2JoinOperator2Perf.containsKey(query)) {
                    query2JoinOperator2Perf.put(query, dp);
                }
                else {
                    HashMap<String, Double> existingDP = query2JoinOperator2Perf.get(query);
                    for (String joinOperator : dp.keySet()) {
                        existingDP.put(joinOperator, dp.get(joinOperator));
                    }
                }
            }
        }
        // check integrity of query2JoinOperator2Perf
        checkQuery2JoinOperator2PerfIntegrity(query2JoinOperator2Perf, appearedJoinOperator, enableFillMissingData);
        if (benchmarks == Benchmarks.TPCH) {
            combineQueriesForTPCH(query2JoinOperator2Perf);
        }
        checkState(hashJoinIncluded, pathToPerformanceRunJsons + "don't contain HASH_JOIN performance");
        List<String> queries = new ArrayList<>(requireNonNull(query2JoinOperator2Perf.keySet()));
        queries.sort((o1, o2) -> {
            String o1name = simplifyQueryName(o1);
            String o2name = simplifyQueryName(o2);
            String o1numStr = o1name.replaceAll("\\D", "");
            String o2numStr = o2name.replaceAll("\\D", "");
            int o1num = o1numStr.isEmpty() ? 0 : Integer.parseInt(o1numStr);
            int o2num = o2numStr.isEmpty() ? 0 : Integer.parseInt(o2numStr);
            if (o1num != o2num) {
                return o1num - o2num;
            }
            return o1name.compareToIgnoreCase(o2name);
        });
        String outputFileName = constructPerfReportFileName(pathToPerformanceRunJsons);
        List<String> headers = new ArrayList<>(List.of("Algorithm"));
        headers.addAll(queries);
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        builder.setHeader(headers.toArray(new String[0]));
        try (Writer writer = new FileWriter(outputFileName, false);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            HashMap<String, List<Double>> joinOperator2Perf = new HashMap<>();
            for (String query : queries) {
                for (String joinOperator : query2JoinOperator2Perf.get(query).keySet()) {
                    joinOperator2Perf.computeIfAbsent(joinOperator, k -> new ArrayList<>());
                    joinOperator2Perf.get(joinOperator).add(query2JoinOperator2Perf.get(query).get(joinOperator));
                }
            }
            for (String joinOperator : joinOperator2Perf.keySet()) {
                List<Object> record = new ArrayList<>(List.of(joinOperator));
                record.addAll(joinOperator2Perf.get(joinOperator));
                csvPrinter.printRecord(record);
            }
            HashMap<String, List<Double>> joinOperator2PerfImprovCmpToHJ = generateJoinOperator2PerfImprovCmpToHJ(joinOperator2Perf);
            for (String key : joinOperator2PerfImprovCmpToHJ.keySet()) {
                List<Object> record = new ArrayList<>(List.of(key));
                record.addAll(joinOperator2PerfImprovCmpToHJ.get(key));
                csvPrinter.printRecord(record);
            }
            System.out.println("result write to: " + outputFileName);
            // generate performance improvement breakdown to console
            List<String> perfImprovementBreakdown = generatePerfImprovementBreakdown(queries,
                    joinOperator2PerfImprovCmpToHJ, showPerfNumber, true, 5);
            for (String output : perfImprovementBreakdown) {
                System.out.println(output);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String constructPerfReportFileName(List<String> pathToPerformanceRunJsons)
    {
        StringBuilder fileName = new StringBuilder();
        String parentPath = Paths.get(pathToPerformanceRunJsons.get(0)).getParent().toString();
        for (String pathToPerformanceRunJson : pathToPerformanceRunJsons) {
            Path pathToPerformanceRunJsonP = Paths.get(pathToPerformanceRunJson);
            fileName.append(FilenameUtils.removeExtension(pathToPerformanceRunJsonP.getName(pathToPerformanceRunJsonP.getNameCount() - 1).toString()));
        }
        String candidateFileName = fileName + "_perf_report.csv";
        if (candidateFileName.length() > 255) {
            candidateFileName = Instant.now().toString() + "_perf_report.csv";
        }
        return Paths.get(parentPath,
                candidateFileName).toString();
    }

    private static void checkQuery2JoinOperator2PerfIntegrity(HashMap<String, HashMap<String, Double>> query2JoinOperator2Perf,
                                                              Set<String> appearedJoinOperator,
                                                              boolean enableFillMissingData)
    {
        int numberOfJoinOperator = appearedJoinOperator.size();
        boolean isMissingData = false;
        for (String query : query2JoinOperator2Perf.keySet()) {
            HashMap<String, Double> dp = query2JoinOperator2Perf.get(query);
            if (dp.size() != numberOfJoinOperator) {
                isMissingData = true;
                List<String> missingJoinOperatorMetrics = new ArrayList<>(appearedJoinOperator);
                for (String joinOperator : dp.keySet()) {
                    missingJoinOperatorMetrics.remove(joinOperator);
                }
                System.out.printf("%s has missing data (missing %s metrics)%n", query, missingJoinOperatorMetrics);
                if (enableFillMissingData) {
                    for (String missingJoinOperator : missingJoinOperatorMetrics) {
                        dp.put(missingJoinOperator, missingDataTimeOut);
                        System.out.printf("Fill missing data (query: %s, metrics: %s) with default value\n", query, missingJoinOperator);
                    }
                }
            }
        }
        if (isMissingData && !enableFillMissingData) {
            throw new RuntimeException("Missing data");
        }
    }

    private static HashMap<String, List<Double>> generateJoinOperator2PerfImprovCmpToHJ(HashMap<String, List<Double>> joinOperator2Perf)
    {
        // start to compute performance improvement percentage
        List<Double> hashJoinPerformance = joinOperator2Perf.get("HASH_JOIN");
        int numPerfs = hashJoinPerformance.size();
        HashMap<String, List<Double>> joinOperator2PerfImprovCmpToHJ = new HashMap<>();
        for (String joinOperator : joinOperator2Perf.keySet()) {
            if (!joinOperator.equals(HASH_JOIN.name())) {
                String key = joinOperator + " perf improv over HJ";
                joinOperator2PerfImprovCmpToHJ.computeIfAbsent(key, k -> new ArrayList<>());
                for (int i = 0; i < numPerfs; ++i) {
                    joinOperator2PerfImprovCmpToHJ.get(key).add(
                            (hashJoinPerformance.get(i) - joinOperator2Perf.get(joinOperator).get(i))
                                    / hashJoinPerformance.get(i));
                }
            }
        }
        return joinOperator2PerfImprovCmpToHJ;
    }

    /**
     * For TPC-H, we combine
     * - 7a with 7b
     * - 19a, 19b, 19c together
     */
    private static void combineQueriesForTPCH(HashMap<String, HashMap<String, Double>> query2JoinOperator2Perf)
    {
        HashMap<String, Double> query7aJoinOperator2Perf = query2JoinOperator2Perf.get("Query7aW");
        HashMap<String, Double> query7bJoinOperator2Perf = query2JoinOperator2Perf.get("Query7bW");
        HashMap<String, Double> query7JoinOperator2Perf = new HashMap<>();
        for (String joinOperator : query7aJoinOperator2Perf.keySet()) {
            Double sum = query7aJoinOperator2Perf.get(joinOperator) + query7bJoinOperator2Perf.get(joinOperator);
            query7JoinOperator2Perf.put(joinOperator, sum);
        }
        query2JoinOperator2Perf.put("Query7W", query7JoinOperator2Perf);
        query2JoinOperator2Perf.remove("Query7aW");
        query2JoinOperator2Perf.remove("Query7bW");

        HashMap<String, Double> query19aJoinOperator2Perf = query2JoinOperator2Perf.get("Query19aW");
        HashMap<String, Double> query19bJoinOperator2Perf = query2JoinOperator2Perf.get("Query19bW");
        HashMap<String, Double> query19cJoinOperator2Perf = query2JoinOperator2Perf.get("Query19cW");
        HashMap<String, Double> query19JoinOperator2Perf = new HashMap<>();
        for (String joinOperator : query19aJoinOperator2Perf.keySet()) {
            Double sum = query19aJoinOperator2Perf.get(joinOperator) +
                    query19bJoinOperator2Perf.get(joinOperator) +
                    query19cJoinOperator2Perf.get(joinOperator);
            query19JoinOperator2Perf.put(joinOperator, sum);
        }
        query2JoinOperator2Perf.put("Query19W", query19JoinOperator2Perf);
        query2JoinOperator2Perf.remove("Query19aW");
        query2JoinOperator2Perf.remove("Query19bW");
        query2JoinOperator2Perf.remove("Query19cW");
    }

    private static List<String> generatePerfImprovementBreakdown(List<String> queries,
                                                                 HashMap<String, List<Double>> joinOperator2PerfImprovCmpToHJ,
                                                                 boolean showPerfNumber,
                                                                 boolean convertToPercentage,
                                                                 long stepSize)
    {
        List<String> output = new ArrayList<>();
        for (String joinOperator : joinOperator2PerfImprovCmpToHJ.keySet()) {
            int countImprovementGreaterThanEqualToZero = 0;
            int countImprovementLessThanZero = 0;
            StringBuilder builder = new StringBuilder();
            builder.append(joinOperator)
                    .append("\n");
            List<Double> perfImprovement = joinOperator2PerfImprovCmpToHJ.get(joinOperator);
            List<Double> convertToPercent = perfImprovement;
            if (convertToPercentage) {
                convertToPercent = perfImprovement.stream().map(k -> k * 100).collect(Collectors.toList());
            }
            Double max = Collections.max(convertToPercent);
            Long roundUpToMultipleOf5Max = stepSize * (Math.round(max / stepSize));
            Double min = Collections.min(convertToPercent);
            Double roundUpToMultipleOf5Min = stepSize * (Math.floor(min / stepSize));
            double binSize = (double) stepSize;
            int numBins = (int) ((roundUpToMultipleOf5Max - roundUpToMultipleOf5Min) / binSize);
            List<List<String>> histogram = new ArrayList<>();
            List<String> title = new ArrayList<>();
            long accumulatedBins = roundUpToMultipleOf5Max + stepSize;
            List<Long> bins = new ArrayList<>();
            for (int j = 0; j < numBins + 2; j++) {
                histogram.add(new ArrayList<>());
                accumulatedBins = accumulatedBins - stepSize;
                bins.add(accumulatedBins);
                if (j == 0) {
                    if (convertToPercentage) {
                        title.add(accumulatedBins + "%+");
                    }
                    else {
                        title.add(accumulatedBins + "+");
                    }
                }
                else if (j == numBins + 1) {
                    if (convertToPercentage) {
                        title.add(accumulatedBins + stepSize + "%+");
                    }
                    else {
                        title.add(accumulatedBins + stepSize + "+");
                    }
                }
                else {
                    if (convertToPercentage) {
                        title.add(String.format("%s%% ~ %s%%", accumulatedBins, accumulatedBins + stepSize));
                    }
                    else {
                        title.add(String.format("%s ~ %s", accumulatedBins, accumulatedBins + stepSize));
                    }
                }
            }
            Collections.sort(bins);
            Collections.reverse(bins);
            for (int i = 0; i < queries.size(); ++i) {
                double data = convertToPercent.get(i);
                if (data >= 0) {
                    countImprovementGreaterThanEqualToZero++;
                }
                else {
                    countImprovementLessThanZero++;
                }
                String query = simplifyQueryName(queries.get(i));
                if (showPerfNumber) {
                    if (convertToPercentage) {
                        query += String.format("(%s%%)", convertToPercent.get(i));
                    }
                    else {
                        query += String.format("(%s)", convertToPercent.get(i));
                    }
                }
                int j = 0;
                for (Long bin : bins) {
                    if (data > bin) {
                        histogram.get(j).add(query);
                        break;
                    }
                    j++;
                }
            }
            builder.append("number of queries have improvement >= 0: " + countImprovementGreaterThanEqualToZero)
                    .append("\n")
                    .append("number of queries have improvement < 0: " + countImprovementLessThanZero)
                    .append("\n");
            for (int i = 0; i < histogram.size(); ++i) {
                builder.append(title.get(i))
                        .append(" (")
                        .append(histogram.get(i).size())
                        .append(") ")
                        .append(":")
                        .append(Joiner.on(",").join(histogram.get(i)))
                        .append("\n");
            }
            output.add(builder.toString());
        }
        return output;
    }

    // TODO: only work for JOB for now
    private static String simplifyQueryName(String query)
    {
        return StringUtils.substringBefore(query, "OptJoinTreeOptOrdering").replace("Query", "");
    }

    private static CSVMeta generateAggregateStatisticsReport(String aggregateStatisticsPath,
                                                             Benchmarks benchmark,
                                                             JoinOperator joinOperator,
                                                             TargetStatsConstraints targetStatsConstraints)
            throws IOException, ParseException
    {
        File[] files = getAListOfTargetJsonFiles(aggregateStatisticsPath, benchmark,
                joinOperator, targetStatsConstraints);
        List<DataFields> totalDataFields = new ArrayList<>();
        for (File jsonFile : files) {
            System.out.println("processing " + jsonFile.getName() + " ...");
            DataFields dataFields = extractFields(jsonFile, joinOperator, benchmark, targetStatsConstraints);
            totalDataFields.add(dataFields);
        }
        return generateAggregateStatisticsCSV(aggregateStatisticsPath, benchmark, totalDataFields, joinOperator, targetStatsConstraints);
    }

    private static CSVMeta generateAggregateStatisticsCSV(String aggregateStatisticsPath,
                                                          Benchmarks benchmark,
                                                          List<DataFields> totalDataFields,
                                                          JoinOperator joinOperator,
                                                          TargetStatsConstraints targetStatsConstraints)
    {
        totalDataFields.sort((o1, o2) -> {
            String o1name = o1.queryName;
            String o2name = o2.queryName;
            String o1numStr = o1name.replaceAll("\\D", "");
            String o2numStr = o2name.replaceAll("\\D", "");
            int o1num = o1numStr.isEmpty() ? 0 : Integer.parseInt(o1numStr);
            int o2num = o2numStr.isEmpty() ? 0 : Integer.parseInt(o2numStr);
            if (o1num != o2num) {
                return o1num - o2num;
            }
            return o1name.compareToIgnoreCase(o2name);
        });
        String outputFileNameBase = joinOperator.name() + "_" + benchmark;
        if (targetStatsConstraints.hashJoinOnYAOrdering) {
            checkState(!targetStatsConstraints.hashJoinOnTTJOrdering);
            outputFileNameBase = outputFileNameBase + "_hashJoinOnYAOrdering";
        }
        else if (targetStatsConstraints.hashJoinOnTTJOrdering) {
            outputFileNameBase = outputFileNameBase + "_hashJoinOnTTJOrdering";
        }
        String outputFileName = Paths.get(aggregateStatisticsPath,
                outputFileNameBase + "_aggregagateStatistics.csv").toString();
        List<String> headers = new ArrayList<>(List.of("statistics"));
        headers.addAll(totalDataFields.stream().map(DataFields::getQueryName).collect(Collectors.toList()));
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        builder.setHeader(headers.toArray(new String[0]));
        List<String> fields = new ArrayList<>();
        try (Writer writer = new FileWriter(outputFileName, false);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            for (Field field : DataFields.class.getDeclaredFields()) {
                field.setAccessible(true);
                if (!field.getName().equals("queryName")) {
                    List<Object> data = new ArrayList<>();
                    data.add(field.getName());
                    fields.add(field.getName());
                    for (DataFields dataFields : totalDataFields) {
                        data.add(field.get(dataFields));
                    }
                    csvPrinter.printRecord(data);
                }
            }
            System.out.println("result write to: " + outputFileName);
        }
        catch (IOException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        return new CSVMeta(headers, fields);
    }

    private static DataFields extractFields(File jsonFile, JoinOperator joinOperator, Benchmarks benchmarks, TargetStatsConstraints targetStatsConstraints)
            throws IOException, ParseException
    {
        return new DataFields(jsonFile.getAbsolutePath(), joinOperator, benchmarks, targetStatsConstraints);
    }

    @Getter
    private static class DataFields
    {
        long rkRelationSize;
        long totalTuplesRemoved;
        long totalTuplesRemovedRk;
        long totalTuplesRemovedInnerRelations;
        long totalTuplesFiltered;
        long totalIntermediateResultsProduced;
        long numberOfPassContextCalls;
        long numberOfPassContextCallsInnerRelations;
        long numberOfPassContextCallsRk;
        long numberOfInitPassContextCalls;
        long totalInputSizeAfterEvaluation;
        long bloomFiltersProbingTime;
        long buildBloomFiltersTime;
        long fullReducerTime;
        long deleteDanglingTupleFromHTime;
        long noGoodListConstructTime;
        long buildNoGoodListTime;
        long noGoodListProbingTime;
        long evaluationMemoryCostInBytes;
        long noGoodListSizeInBytes;
        long noGoodListSize;

        final String queryName;

        public DataFields(String file, JoinOperator joinOperator, Benchmarks benchmarks, TargetStatsConstraints targetStatsConstraints)
                throws IOException, ParseException
        {
            queryName = setQueryName(file, joinOperator, benchmarks, targetStatsConstraints);
            Object obj = new JSONParser().parse(new FileReader(file));
            JSONObject jo = (JSONObject) obj;
            JSONObject aggStats = (JSONObject) jo.get("Aggregation Stats");
            totalTuplesRemoved = (long) (aggStats).get("totalTuplesRemoved");
            totalIntermediateResultsProduced = (long) (aggStats).get("totalIntermediateResultsProduced");
            try {
                rkRelationSize = (long) (aggStats).get("rkRelationSize");
                totalInputSizeAfterEvaluation = (long) (aggStats).get("totalInputSizeAfterEvaluation");
                evaluationMemoryCostInBytes = (long) (aggStats).get("evaluationMemoryCostInBytes");
            }
            catch (NullPointerException ignored) {
                // TODO: once all the data are generated, we can safely remove this try ... catch
            }
            switch (joinOperator) {
                case TTJHP_NO_NG:
                case TTJHP:
                    totalTuplesRemovedRk = (long) (aggStats).get("totalTuplesRemovedRk");
                    totalTuplesRemovedInnerRelations = (long) (aggStats).get("totalTuplesRemovedInnerRelations");
                    totalTuplesFiltered = (long) (aggStats).get("totalTuplesFiltered");
                    numberOfPassContextCallsInnerRelations = (long) (aggStats).get("numberOfPassContextCallsInnerRelations");
                    numberOfPassContextCallsRk = (long) (aggStats).get("numberOfPassContextCallsRk");
                    numberOfPassContextCalls = (long) (aggStats).get("numberOfPassContextCalls");
                    numberOfInitPassContextCalls = (long) (aggStats).get("numberOfInitPassContextCalls");
                    deleteDanglingTupleFromHTime = (long) (aggStats).get("deleteDanglingTupleFromHTime (ms)");
                    noGoodListConstructTime = (long) (aggStats).get("noGoodListConstructTime (ms)");
                    noGoodListProbingTime = (long) (aggStats).get("noGoodListProbingTime (ms)");
                    buildNoGoodListTime = (long) (aggStats).get("buildNoGoodListTime (ms)");
                    try {
                        noGoodListSizeInBytes = (long) (aggStats).get("noGoodListSizeInBytes");
                        noGoodListSize = (long) (aggStats).get("noGoodListSize");
                    }
                    catch (NullPointerException ignored) {
                        // TODO: once all the data are generated, we can safely remove this try ... catch
                    }
                    break;
                case LIP:
                    bloomFiltersProbingTime = (long) (aggStats).get("bloomFiltersProbingTime (ms)");
                    buildBloomFiltersTime = (long) (aggStats).get("buildBloomFiltersTime (ms)");
                    break;
                case Yannakakis:
                    fullReducerTime = (long) (aggStats).get("fullReducerTime (ms)");
                    break;
            }
        }

        /**
         * Assuming "TTJHP_org.zhu45.treetracker.benchmark.job.q25.Query25cOptJoinTreeOptOrdering.json"
         * if we use the same ordering. In different ordering and HASH_JOIN, the search keyword is *FindOptJoinTree.json.
         * <p></p>
         * The above exception is SSB where we assume "LIP_org.zhu45.treetracker.benchmark.ssb.Query1P1.json"
         * and we directly extract "Query1P1"
         */
        private String setQueryName(String file,
                                    JoinOperator joinOperator,
                                    Benchmarks benchmarks,
                                    TargetStatsConstraints targetStatsConstraints)
        {
            String[] tokens = file.split("\\.");
            if (benchmarks != Benchmarks.SSB) {
                String target = tokens[tokens.length - 2];
                int endIndex = 0;
                if (targetStatsConstraints.hashJoinOnTTJOrdering && joinOperator == HASH_JOIN) {
                    endIndex = target.indexOf("OptJoinTreeOptOrdering");
                }
                else if (targetStatsConstraints.hashJoinOnYAOrdering && joinOperator == HASH_JOIN) {
                    endIndex = target.indexOf("OptJoinTreeOptOrderingHJOnY");
                }
                else if (!targetStatsConstraints.sameOrdering && joinOperator == HASH_JOIN) {
                    endIndex = target.indexOf("FindOptJoinTree");
                }
                else {
                    endIndex = target.indexOf("OptJoinTreeOptOrdering");
                }
                return target.substring(0, endIndex);
            }
            return tokens[tokens.length - 2];
        }
    }

    @Getter
    private static class CSVMeta
    {
        List<String> headers;
        List<String> fields;

        public CSVMeta(List<String> headers, List<String> fields)
        {
            this.headers = headers;
            this.fields = fields;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this) {
                return true;
            }
            if (!(obj instanceof CSVMeta)) {
                return false;
            }
            CSVMeta c = (CSVMeta) obj;
            return Objects.equals(c.headers, this.headers) &&
                    Objects.equals(c.fields, this.fields);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(headers, fields);
        }
    }

    private static String getCanonicalQuery(String rawQuery)
    {
        requireNonNull(rawQuery, "rawQuery is null");
        if (rawQuery.contains("FindOptJoinTree")) {
            int index = rawQuery.indexOf("FindOptJoinTree");
            return rawQuery.substring(0, index);
        }
        else if (rawQuery.contains("OptJoinTreeOptOrdering")) {
            int index = rawQuery.indexOf("OptJoinTreeOptOrdering");
            return rawQuery.substring(0, index);
        }
        return rawQuery;
    }

    private static String getRawQuery(Benchmarks benchmarks, JSONObject group)
    {
        List<String> queryPatterns;
        switch (benchmarks) {
            case JOB:
                queryPatterns = List.of("jobQueries");
                break;
            case TPCH:
                queryPatterns = List.of("tpchQueries", "tpchQueriesFindOptJoinTree", "tpchQueriesYannakakis", "tpchQueriesYannakakisB", "tpchQueriesYannakakisV", "tpchQueriesPTO");
                break;
            case SSB:
                queryPatterns = List.of("ssbQueries", "ssbQueriesFindOptJoinTree");
                break;
            default:
                throw new RuntimeException("unknown benchmark: " + benchmarks);
        }
        for (String queryPattern : queryPatterns) {
            String rawQuery = (String) ((JSONObject) group.get("params")).get(queryPattern);
            if (rawQuery != null) {
                return rawQuery;
            }
        }
        throw new RuntimeException("Cannot extract from " + group + "based on queryPatterns " + queryPatterns);
    }

    private static CSVMeta generateAggregateStatisticsReport(Benchmarks benchmark,
                                                             List<JoinOperator> joinOperators,
                                                             TargetStatsConstraints targetStatsConstraints)
            throws IOException, ParseException
    {
        CSVMeta prevMeta;
        CSVMeta currMeta = null;
        for (JoinOperator joinOperator : joinOperators) {
            prevMeta = currMeta;
            currMeta = generateAggregateStatisticsReport(joinOperator, benchmark, targetStatsConstraints);
            if (prevMeta != null) {
                checkState(prevMeta.equals(currMeta));
            }
        }
        return currMeta;
    }

    private static HashMap<String, List<Double>> generateJoinOperator2SpeedUpCmpToHJ(HashMap<String, List<Double>> joinOperator2Perf)
    {
        // start to compute performance improvement percentage
        List<Double> hashJoinPerformance = joinOperator2Perf.get("HASH_JOIN");
        int numPerfs = hashJoinPerformance.size();
        HashMap<String, List<Double>> joinOperator2PerfImprovCmpToHJ = new HashMap<>();
        for (String joinOperator : joinOperator2Perf.keySet()) {
            if (!joinOperator.equals(HASH_JOIN.name())) {
                String key = joinOperator + " speedup over HJ";
                joinOperator2PerfImprovCmpToHJ.computeIfAbsent(key, k -> new ArrayList<>());
                for (int i = 0; i < numPerfs; ++i) {
                    joinOperator2PerfImprovCmpToHJ.get(key).add(hashJoinPerformance.get(i) / joinOperator2Perf.get(joinOperator).get(i));
                }
            }
        }
        return joinOperator2PerfImprovCmpToHJ;
    }

    private static void produceIntermediateResultsImprovRatioTable(String csvDir,
                                                                   List<JoinOperator> joinOperators,
                                                                   Benchmarks benchmark,
                                                                   CSVMeta csvMeta)
            throws IOException
    {
        System.out.println("Producing IntermediateResultsImprovRatioTable ...");
        HashMap<String, List<Double>> joinOperator2IntermediateResultSize = new HashMap<>();
        checkState(joinOperators.contains(HASH_JOIN), "joinOperators has to contain HASH_JOIN");
        for (String joinOperator : joinOperators.stream().map(Enum::name).collect(Collectors.toList())) {
            try (Reader hashJoinCSV = new FileReader(Paths.get(csvDir, joinOperator + "_" + benchmark + "_aggregagateStatistics.csv").toString())) {
                CSVFormat csvFormat = CSVFormat.DEFAULT.builder()
                        .setHeader(csvMeta.headers.toArray(new String[0]))
                        .setSkipHeaderRecord(true)
                        .build();
                Iterable<CSVRecord> records = csvFormat.parse(hashJoinCSV);
                joinOperator2IntermediateResultSize.computeIfAbsent(joinOperator, k -> new ArrayList<>());
                for (CSVRecord record : records) {
                    if (record.get(0).equals("totalIntermediateResultsProduced")) {
                        for (int i = 1; i < record.size(); ++i) {
                            joinOperator2IntermediateResultSize.get(joinOperator).add(Double.parseDouble(record.get(i)));
                        }
                        break;
                    }
                }
            }
        }
        System.out.println(generateTable(joinOperator2IntermediateResultSize,
                csvMeta.headers, "totalIntermediateResultsProduced"));
        HashMap<String, List<Double>> joinOperator2SpeedUpCmpToHJ = generateJoinOperator2SpeedUpCmpToHJ(joinOperator2IntermediateResultSize);
        // generate performance improvement breakdown to console
        List<String> perfImprovementBreakdown = generatePerfImprovementBreakdown(csvMeta.headers.subList(1, csvMeta.headers.size()),
                joinOperator2SpeedUpCmpToHJ, true,
                false, 100);
        for (String output : perfImprovementBreakdown) {
            System.out.println(output);
        }
    }

    private static String generateTable(HashMap<String, List<Double>> joinOperator2Data,
                                        List<String> header,
                                        String title)
    {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(title)
                .append("\n");
        AsciiTable at = new AsciiTable();
        at.addRule();
        at.addRow(header);
        at.addRule();
        for (String joinOperator : joinOperator2Data.keySet()) {
            List<Object> row = new ArrayList<>();
            row.add(joinOperator);
            row.addAll(joinOperator2Data.get(joinOperator));
            at.addRow(row);
            at.addRule();
        }
        at.setTextAlignment(TextAlignment.LEFT);
        stringBuilder.append(at.render(header.size() * 10));
        return stringBuilder.toString();
    }

    private static void analyzeAggregateStatisticsReport(String csvDir,
                                                         List<JoinOperator> joinOperators,
                                                         Benchmarks benchmark,
                                                         CSVMeta csvMeta)
            throws IOException
    {
        produceIntermediateResultsImprovRatioTable(csvDir, joinOperators, benchmark, csvMeta);
    }

    private static void analyzeAggregateStatisticsReport(boolean sameOrdering,
                                                         List<JoinOperator> joinOperators,
                                                         Benchmarks benchmark,
                                                         boolean useTrueCard,
                                                         CSVMeta csvMeta)
            throws IOException
    {
        String path = decideAggStatsPath(sameOrdering, useTrueCard);
        analyzeAggregateStatisticsReport(path, joinOperators, benchmark, csvMeta);
    }

    private static String decideAggStatsPath(boolean sameOrdering, boolean useTrueCard)
    {
        String path = sameOrdering ? SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_SAME_ORDERING_STORED_PATH :
                SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_STORED_PATH;
        return useTrueCard ? SIMPLE_COST_MODEL_RESULT_WITH_PREDICATES_TRUE_CARD_STORED_PATH : path;
    }

    private static List<String> constructPaths(List<String> jsonFileNames, Benchmarks benchmark)
    {
        switch (benchmark) {
            case JOB:
                return jsonFileNames.stream().map(jsonFileName -> Paths.get(JOB_WITH_PREDICATES_RESULT_STORED_PATH, jsonFileName).toString()).collect(Collectors.toList());
            case TPCH:
                return jsonFileNames.stream().map(jsonFileName -> Paths.get(TPCH_WITH_PREDICATES_RESULT_STORED_PATH, jsonFileName).toString()).collect(Collectors.toList());
            case SSB:
                return jsonFileNames.stream().map(jsonFileName -> Paths.get(SSB_RESULT_STORED_PATH, jsonFileName).toString()).collect(Collectors.toList());
        }
        throw new RuntimeException();
    }

    @Getter
    private static class TargetStatsConstraints
    {
        private final boolean sameOrdering;
        private final boolean useTrueCard;
        private final boolean hashJoinOnTTJOrdering;
        private final boolean hashJoinOnYAOrdering;

        private TargetStatsConstraints(Builder builder)
        {
            this.sameOrdering = builder.sameOrdering;
            this.useTrueCard = builder.useTrueCard;
            this.hashJoinOnTTJOrdering = builder.hashJoinOnTTJOrdering;
            this.hashJoinOnYAOrdering = builder.hashJoinOnYAOrdering;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class Builder
        {
            private boolean sameOrdering;
            private boolean useTrueCard;
            private boolean hashJoinOnTTJOrdering;
            private boolean hashJoinOnYAOrdering;

            public Builder()
            {
            }

            public Builder setSameOrdering(boolean sameOrdering)
            {
                this.sameOrdering = sameOrdering;
                return this;
            }

            public Builder setUseTrueCard(boolean useTrueCard)
            {
                this.useTrueCard = useTrueCard;
                return this;
            }

            public Builder setHashJoinOnTTJOrdering(boolean hashJoinOnTTJOrdering)
            {
                this.hashJoinOnTTJOrdering = hashJoinOnTTJOrdering;
                return this;
            }

            public Builder setHashJoinOnYAOrdering(boolean hashJoinOnYAOrdering)
            {
                this.hashJoinOnYAOrdering = hashJoinOnYAOrdering;
                return this;
            }

            public TargetStatsConstraints build()
            {
                return new TargetStatsConstraints(this);
            }
        }
    }

    private static void generateReportSSB()
            throws IOException, ParseException
    {
        List<JoinOperator> ssbJoinOperators = List.of(LIP, HASH_JOIN, Yannakakis, TTJHP, YannakakisB);
        TargetStatsConstraints constraints = TargetStatsConstraints.builder()
                .setSameOrdering(false)
                .setUseTrueCard(false)
                .build();
        CSVMeta csvMeta = generateAggregateStatisticsReport(Benchmarks.SSB, ssbJoinOperators,
                constraints);
//        analyzeAggregateStatisticsReport(false, ssbJoinOperators, Benchmarks.SSB, false, csvMeta);
    }

    private static void generateReportTPCH()
            throws IOException, ParseException
    {
        List<JoinOperator> tpchJoinOperators;
        CSVMeta csvMeta;
        TargetStatsConstraints constraints;
        tpchJoinOperators = List.of(HASH_JOIN, TTJHP, Yannakakis, YannakakisB);
        constraints = TargetStatsConstraints.builder()
                .setSameOrdering(false)
                .setUseTrueCard(false)
                .build();
        csvMeta = generateAggregateStatisticsReport(Benchmarks.TPCH, tpchJoinOperators,
                constraints);
        tpchJoinOperators = List.of(HASH_JOIN);
        constraints = TargetStatsConstraints.builder()
                .setSameOrdering(false)
                .setUseTrueCard(false)
                .setHashJoinOnTTJOrdering(true)
                .build();
        csvMeta = generateAggregateStatisticsReport(Benchmarks.TPCH, tpchJoinOperators,
                constraints);
        constraints = TargetStatsConstraints.builder()
                .setSameOrdering(false)
                .setUseTrueCard(false)
                .setHashJoinOnYAOrdering(true)
                .build();
        csvMeta = generateAggregateStatisticsReport(Benchmarks.TPCH, tpchJoinOperators,
                constraints);
    }

    private static void generateReportJOB()
            throws IOException, ParseException
    {
        List<JoinOperator> jobJoinOperators = List.of(HASH_JOIN, TTJHP, TTJHP_NO_NG);
        TargetStatsConstraints constraints = TargetStatsConstraints.builder()
                .setSameOrdering(false)
                .setUseTrueCard(false)
                .build();
        CSVMeta csvMeta = generateAggregateStatisticsReport(Benchmarks.JOB,
                jobJoinOperators,
                constraints);
        analyzeAggregateStatisticsReport(false, jobJoinOperators, Benchmarks.JOB, false, csvMeta);
    }

    public static void main(String[] args)
            throws IOException, ParseException
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(GenerateBenchmarkStatisticsReport.class.getName(), Level.DEBUG);
        }
        generateReportJOB();
        generatePerformanceReport(constructPaths(List.of("benchmarkjobwithpredicatesdifferentordering-result-2023-06-23t17:47:56.224875.json",
                        // Fixed 14a result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-26t16:34:57.399775.json",
                        // Fixed 14c result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-26t17:49:45.003226.json",
                        // Fixed 22b result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-27t13:38:47.587592.json",
                        // Fixed 22a result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-27t14:47:33.681481.json",
                        // Fixed 22c and 22d result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-27t16:29:24.848821.json",
                        // Fixed 13a,13b,13c,28a,28b,28c,30a,30b,30c result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-29t05:50:20.711411.json",
                        // Fixed 7a,7b,7c,12a,12b,12c,18a,18b,18c,25a,25b,25c,31a,31b,31c result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-29t22:05:07.536835.json",
                        // Fixed 13d result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-06-27t19:09:14.165813.json",
                        // Fixed 20b result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-11-26t16:18:48.020256.json",
                        // Yannakakis result
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-07-30t17:19:32.878615.json",
                        // Add TTJHP_NO_NG result (to compare with TTJHP to see if TTJ Scan matters for the performance on JOB)
                        // Note TTJHP_NO_NG uses the same ordering and join tree as TTJHP.
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-08-16t13:09:03.748113.json",
                        // Add TTJHP_BF result (TTJHP_BF uses the same ordering and join tree as TTJHP)
                        Paths.get("exp2.3", "exp2p3job-result-2023-11-20t19:05:06.777673.json").toString(),
                        // TTJHP_BF missing 10a result
                        Paths.get("exp2.3", "exp2p3job-result-2023-11-22t12:59:33.005234.json").toString(),
                        // TTJHP_BF re-run 4b and see if it's still the regression --> it is
                        Paths.get("exp2.3", "exp2p3job-result-2023-11-22t13:29:36.302887.json").toString(),
                        // TTJHP_BG result
                        Paths.get("exp2.3", "exp2p3job-result-2023-11-27t22:00:58.098241.json").toString(),
                        // Update YA (after optimization) results with one query from each query flight
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-12-21t00:00:54.637272.json",
                        // TTJHP, HJ, Yannakakis, YannakakisB (HJPhysicalHeuristics is enabled)
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-12-30t16:43:27.406285.json"), Benchmarks.JOB),
                Benchmarks.JOB, true, false);
        // We need to run a separate generatePerformanceReport() for fixed HJ ordering because fixed HJ ordering has
        // TTJHP, which will override TTJHP on different ordering results when building joinOperator2Perf if fixed HJ ordering
        // is part of above generatePerformanceReport() call json files list.
        generatePerformanceReport(constructPaths(List.of(
                        // TTJHP, HJ, Yannakakis, YannakakisB (HJPhysicalHeuristics is enabled)
                        "benchmarkjobwithpredicatesdifferentordering-result-2023-12-30t16:43:27.406285.json",
                        // TTHP on fixed HJ ordering
                        Paths.get("hj_ordering_opt_jointree", "benchmarkjobwithpredicatesfixedhjordering-result-2024-01-07t00:12:12.344309.json").toString()), Benchmarks.JOB),
                Benchmarks.JOB, true, false);
        generatePerformanceReport(constructPaths(List.of("benchmarktpchwithpredicatesdifferentordering-result-2023-07-06t17:42:23.652904.json",
                        // Fixed 15 implementation result
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-07-07t13:48:17.442581.json",
                        // Yannakakis and YannakakisB results
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-15t21:30:43.174293.json",
                        // Yannakakis, YannakakisB, and YannakakisV results
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-17t01:47:57.250707.json",
                        // YannakakisB (with disablePTOptimizationTrick = true)
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-17t22:43:09.581579.json",
                        // Yannakakis, YannakakisB, and YannakakisV results after fixing pipelining bug
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-18t15:24:53.96508.json",
                        // PTO result
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-20t13:50:29.163599.json",
                        // TTJ on Q8W (HJPhysicalHeuristics is enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-21t16:05:42.756349.json",
                        // TTJ, HJ, Yannakakis, YannakakisB on Q8W (HJPhysicalHeuristics is enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-21t17:15:44.636482.json",
                        // TTJ, HJ, Yannakakis, YannakakisB, PTO on TPC-H queries (HJPhysicalHeuristics enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-21t20:10:26.588849.json",
                        // Fixed YannakakisB on TPC-H queries (HJPhysicalHeuristics enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2023-12-23t00:40:10.130498.json",
                        // Fixed TTJHP, HJ, Yannakakis, and YannakakisB on 16W due to query implementation error (HJPhysicalHeuristics enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2024-01-09t22:29:38.123331.json",
                        // Fixed TTJHP, HJ, Yannakakis, and YannakakisB on 19aW, 19bW, 19cW due to query implementation error (HJPhysicalHeuristics enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2024-01-10t11:25:55.997022.json",
                        // Fixed HJ ordering on 18W due to overestimation (HJPhysicalHeuristics enabled)
                        "benchmarktpchwithpredicatesdifferentordering-result-2024-01-11t21:43:14.826502.json"), Benchmarks.TPCH),
                Benchmarks.TPCH, true, false);
        generateReportTPCH();
        generatePerformanceReport(constructPaths(List.of("benchmarkssb-result-2023-10-26t12:14:04.768421.json",
                        // Benchmark result use SSB-specific LIP implementation
                        "benchmarkssb-result-2023-12-08t23:44:40.399941.json",
                        // Yannakakis and YannakakisB results
                        "benchmarkssb-result-2023-12-15t15:01:48.153382.json",
                        // YannakakisB results with new Bloom filter
                        "benchmarkssb-result-2023-12-16t21:16:30.867949.json",
                        // YannakakisB results with new Bloom filter V2
                        "benchmarkssb-result-2023-12-16t23:32:08.354566.json",
                        // Yannakakis and YannakakisB results after fixing pipelining bug
                        "benchmarkssb-result-2023-12-19t01:39:55.3923.json",
                        // LIP results on Blocked Bloom
                        "benchmarkssb-result-2024-01-04t23:46:46.3084.json"),
                Benchmarks.SSB), Benchmarks.SSB, true, false);
        generateReportSSB();
    }
}
