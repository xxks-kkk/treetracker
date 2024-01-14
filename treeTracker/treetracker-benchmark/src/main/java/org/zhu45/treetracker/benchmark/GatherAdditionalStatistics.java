package org.zhu45.treetracker.benchmark;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treetracker.benchmark.job.q1.Query13;
import org.zhu45.treetracker.benchmark.job.q10.Query103;
import org.zhu45.treetracker.benchmark.job.q11.Query11;
import org.zhu45.treetracker.benchmark.job.q12.Query12;
import org.zhu45.treetracker.benchmark.job.q14.Query146;
import org.zhu45.treetracker.benchmark.job.q15.Query153;
import org.zhu45.treetracker.benchmark.job.q16.Query161;
import org.zhu45.treetracker.benchmark.job.q17.Query172;
import org.zhu45.treetracker.benchmark.job.q18.Query181;
import org.zhu45.treetracker.benchmark.job.q19.Query191;
import org.zhu45.treetracker.benchmark.job.q2.Query2;
import org.zhu45.treetracker.benchmark.job.q20.Query20;
import org.zhu45.treetracker.benchmark.job.q21.Query211;
import org.zhu45.treetracker.benchmark.job.q22.Query22;
import org.zhu45.treetracker.benchmark.job.q23.Query23;
import org.zhu45.treetracker.benchmark.job.q25.Query25;
import org.zhu45.treetracker.benchmark.job.q26.Query26;
import org.zhu45.treetracker.benchmark.job.q27.Query273;
import org.zhu45.treetracker.benchmark.job.q28.Query28;
import org.zhu45.treetracker.benchmark.job.q3.Query32;
import org.zhu45.treetracker.benchmark.job.q30.Query30;
import org.zhu45.treetracker.benchmark.job.q31.Query311;
import org.zhu45.treetracker.benchmark.job.q33.Query331;
import org.zhu45.treetracker.benchmark.job.q4.Query44;
import org.zhu45.treetracker.benchmark.job.q5.Query53;
import org.zhu45.treetracker.benchmark.job.q6.Query62;
import org.zhu45.treetracker.benchmark.job.q7.Query7;
import org.zhu45.treetracker.benchmark.job.q8.Query84;
import org.zhu45.treetracker.benchmark.job.q9.Query9;
import org.zhu45.treetracker.relational.operator.AggregateStatisticsInformation;
import org.zhu45.treetracker.relational.operator.JoinOperator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Optional;

import static org.zhu45.treetracker.benchmark.QueryProvider.gatherQueryStatistics;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_AGG_STATS_LOC;
import static org.zhu45.treetracker.common.TestConstants.getEnvVariableSet;

/**
 * We gather additional execution statistics for each algorithm for each query.
 */
public class GatherAdditionalStatistics
{
    private GatherAdditionalStatistics() {}

    public static void runAndGather(JoinOperator joinOperator, Class<? extends Query> queryClazz)
    {
        String outputFileName = getEnvVariableSet(TREETRACKER_AGG_STATS_LOC);
        AggregateStatisticsInformation aggregateStatisticsInformation = gatherQueryStatistics(joinOperator, queryClazz, Optional.empty());
        String[] headers = aggregateStatisticsInformation.getHeader();
        File f = new File(outputFileName);
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        if (f.length() == 0) {
            builder.setHeader(headers);
        }
        try (Writer writer = new FileWriter(outputFileName, true);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            csvPrinter.printRecord(aggregateStatisticsInformation.getVals());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Pair<JoinOperator, Class<? extends Query>> getInputPairJob(String query, String algorithm)
    {
        JoinOperator joinOperator = JoinOperator.valueOf(algorithm);
        switch (query) {
            case "Q1":
                return Pair.of(joinOperator, Query13.class);
            case "Q2":
                return Pair.of(joinOperator, Query2.class);
            case "Q3":
                return Pair.of(joinOperator, Query32.class);
            case "Q4":
                return Pair.of(joinOperator, Query44.class);
            case "Q5":
                return Pair.of(joinOperator, Query53.class);
            case "Q6":
                return Pair.of(joinOperator, Query62.class);
            case "Q7":
                return Pair.of(joinOperator, Query7.class);
            case "Q8":
                return Pair.of(joinOperator, Query84.class);
            case "Q9":
                return Pair.of(joinOperator, Query9.class);
            case "Q10":
                return Pair.of(joinOperator, Query103.class);
            case "Q11":
                return Pair.of(joinOperator, Query11.class);
            case "Q12":
                return Pair.of(joinOperator, Query12.class);
            case "Q13":
                return Pair.of(joinOperator, org.zhu45.treetracker.benchmark.job.q13.Query13.class);
            case "Q14":
                return Pair.of(joinOperator, Query146.class);
            case "Q15":
                return Pair.of(joinOperator, Query153.class);
            case "Q16":
                return Pair.of(joinOperator, Query161.class);
            case "Q17":
                return Pair.of(joinOperator, Query172.class);
            case "Q18":
                return Pair.of(joinOperator, Query181.class);
            case "Q19":
                return Pair.of(joinOperator, Query191.class);
            case "Q20":
                return Pair.of(joinOperator, Query20.class);
            case "Q21":
                return Pair.of(joinOperator, Query211.class);
            case "Q22":
                return Pair.of(joinOperator, Query22.class);
            case "Q23":
                return Pair.of(joinOperator, Query23.class);
            case "Q25":
                return Pair.of(joinOperator, Query25.class);
            case "Q26":
                return Pair.of(joinOperator, Query26.class);
            case "Q27":
                return Pair.of(joinOperator, Query273.class);
            case "Q28":
                return Pair.of(joinOperator, Query28.class);
            case "Q30":
                return Pair.of(joinOperator, Query30.class);
            case "Q31":
                return Pair.of(joinOperator, Query311.class);
            case "Q32":
                return Pair.of(joinOperator, org.zhu45.treetracker.benchmark.job.q32.Query32.class);
            case "Q33":
                return Pair.of(joinOperator, Query331.class);
            default:
                throw new IllegalArgumentException("Given query " + query + " is unsupported!");
        }
    }

    public static void main(String[] args)
    {
        String query = args[0];
        String algorithm = args[1];
        System.out.printf("Start %s %s%n", query, algorithm);
        Pair<JoinOperator, Class<? extends Query>> pair = getInputPairJob(query, algorithm);
        runAndGather(pair.getLeft(), pair.getRight());
    }
}
