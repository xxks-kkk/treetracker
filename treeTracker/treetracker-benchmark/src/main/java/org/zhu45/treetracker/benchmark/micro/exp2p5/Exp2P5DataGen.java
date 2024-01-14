package org.zhu45.treetracker.benchmark.micro.exp2p5;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.Benchmarks.EXP2P5_RESULT_STORED_PATH;

/***
 * This class generates relation data in csv form for benchmarking
 */
public class Exp2P5DataGen
{
    private Exp2P5DataGen()
    {
    }

    private static int numberOfTuplesInT = 10000000;
    private static int seed = 10;
    private static Random rand = new Random(seed);

    private static void generateExp2P5T(int dupratio)
    {
        List<Integer> columnA = new ArrayList<>(numberOfTuplesInT);
        for (int i = 0; i < numberOfTuplesInT; i++) {
            columnA.add(i);
        }
        Collections.shuffle(columnA, rand);
        List<Integer> columnB = new ArrayList<>(numberOfTuplesInT);
        // Set A size
        int numberOfDanglingTuplesCanBeFilteredOutByNg = (int) Math.round(dupratio / 100.0 * numberOfTuplesInT);
        if (numberOfDanglingTuplesCanBeFilteredOutByNg == numberOfTuplesInT) {
            numberOfDanglingTuplesCanBeFilteredOutByNg = numberOfTuplesInT - 1;
        }
        // Set B size
        int numberOfDanglingTuplesAreUnique = numberOfTuplesInT - numberOfDanglingTuplesCanBeFilteredOutByNg;
        for (int i = 0; i < numberOfDanglingTuplesAreUnique; i++) {
            columnB.add(i);
        }
        for (int i = 0; i < numberOfDanglingTuplesCanBeFilteredOutByNg; ++i) {
            columnB.add(columnB.get(rand.nextInt(numberOfDanglingTuplesAreUnique)));
        }
        Collections.shuffle(columnB, rand);
        checkState(columnA.size() == columnB.size(),
                "columnA and columnB should have the same length");
        writeCSV("exp2.5T_" + dupratio + ".csv", columnA, columnB);
    }

    private static void generateExp2P5R()
    {
        List<Integer> columnA = new ArrayList<>(numberOfTuplesInT);
        for (int i = 0; i < numberOfTuplesInT; i++) {
            columnA.add(i);
        }
        Collections.shuffle(columnA, rand);
        writeCSV("exp2.5R" + ".csv", columnA);
    }

    private static void generateExp2P5S()
    {
        List<Integer> columnB = new ArrayList<>(numberOfTuplesInT);
        for (int i = numberOfTuplesInT; i < 2 * numberOfTuplesInT; i++) {
            columnB.add(i);
        }
        Collections.shuffle(columnB, rand);
        checkState(columnB.size() == numberOfTuplesInT);
        writeCSV("exp2.5S" + ".csv", columnB);
    }

    @SafeVarargs
    private static void writeCSV(String fileName, List<Integer>... columns)
    {
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        String outputFileName = Paths.get(EXP2P5_RESULT_STORED_PATH, fileName).toString();
        try (Writer writer = new FileWriter(outputFileName, false);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            int columnLength = columns[0].size();
            for (int i = 0; i < columnLength; ++i) {
                List<Integer> row = new ArrayList<>(columns.length);
                for (List<Integer> column : columns) {
                    row.add(column.get(i));
                }
                csvPrinter.printRecord(row);
            }
            System.out.println("result write to: " + outputFileName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void run(int dupratio)
    {
        generateExp2P5T(dupratio);
        generateExp2P5R();
        generateExp2P5S();
    }

    public static void main(String[] args)
    {
        List<Integer> dupratioList = List.of(0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100);
        for (int dupratio : dupratioList) {
            run(dupratio);
        }
    }
}
