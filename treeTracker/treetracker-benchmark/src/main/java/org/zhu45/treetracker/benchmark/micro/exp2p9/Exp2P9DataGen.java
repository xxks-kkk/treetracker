package org.zhu45.treetracker.benchmark.micro.exp2p9;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.zhu45.treetracker.benchmark.Benchmarks.EXP2P9_RESULT_STORED_PATH;

/***
 * This class generates relation data in csv form for benchmarking
 */
public class Exp2P9DataGen
{
    private Exp2P9DataGen()
    {
    }

    private static int numberOfTuplesInU = 1000000;
//    private static int numberOfTuplesInU = 10;

    private static void generateExp2P9U(int semijoinmodratio)
    {
        List<List<Integer>> relationInstance = new ArrayList<>(numberOfTuplesInU);
        int numDanglingTuplesInT = Math.max(1, (int) Math.round(semijoinmodratio / 100.0 * numberOfTuplesInU));
        for (int i = 0; i < numberOfTuplesInU; i++) {
            if (i < numDanglingTuplesInT) {
                relationInstance.add(List.of(2, i + 1));
            }
            else if (i != numberOfTuplesInU - 1) {
                relationInstance.add(List.of(3, i + 1));
            }
            else {
                relationInstance.add(List.of(4, 4));
            }
        }
        writeCSV("exp2.9U_" + semijoinmodratio + ".csv", relationInstance);
    }

    private static void generateExp2P9R()
    {
        writeCSV("exp2.9R" + ".csv", List.of(List.of(1, 2), List.of(1, 4)));
    }

    private static void generateExp2P9V()
    {
        writeCSV("exp2.9V" + ".csv", List.of(List.of(2, 3), List.of(4, 4)));
    }

    private static void generateExp2P9W()
    {
        writeCSV("exp2.9W" + ".csv", List.of(List.of(4, 5)));
    }

    private static void writeCSV(String fileName, List<List<Integer>> relationInstance)
    {
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        String outputFileName = Paths.get(EXP2P9_RESULT_STORED_PATH, fileName).toString();
        try (Writer writer = new FileWriter(outputFileName, false);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            for (List<Integer> tuple : relationInstance) {
                csvPrinter.printRecord(tuple);
            }
            System.out.println("result write to: " + outputFileName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
    {
        generateExp2P9R();
        generateExp2P9V();
        generateExp2P9W();
        List<Integer> semijoinmodratioList = List.of(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
        for (int semijoinmodratio : semijoinmodratioList) {
            generateExp2P9U(semijoinmodratio);
        }
    }
}
