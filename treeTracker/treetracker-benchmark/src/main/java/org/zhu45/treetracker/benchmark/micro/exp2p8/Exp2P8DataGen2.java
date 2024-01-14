package org.zhu45.treetracker.benchmark.micro.exp2p8;

import com.google.common.base.Joiner;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static org.zhu45.treetracker.benchmark.Benchmarks.EXP2P8_RESULT_STORED_PATH;

/**
 * Second database instance described in Exp2.8
 */
public class Exp2P8DataGen2
{
    private Exp2P8DataGen2()
    {
    }

    /**
     * Generate A_1\dotsA_k relation
     */
    private static List<List<Integer>> generateA1DotsAk(int numberOfBackJumpedRelations)
    {
        int numberOfAttributes = numberOfBackJumpedRelations + 2;
        List<List<Integer>> relationInstance = new ArrayList<>();
        List<Integer> tuple = new ArrayList<>(numberOfAttributes);
        relationInstance.add(IntStream.rangeClosed(1, numberOfAttributes)
                .boxed().collect(Collectors.toList()));
        tuple.add(1);
        tuple.addAll(IntStream.rangeClosed(3, numberOfAttributes + 1)
                .boxed().collect(Collectors.toList()));
        relationInstance.add(tuple);
        checkState(relationInstance.get(0).size() == relationInstance.get(1).size());
        return relationInstance;
    }

    private static void writeCSV(String fileName, List<List<Integer>> relationInstance, int beginColumn, int endColumn)
    {
        CSVFormat.Builder builder = CSVFormat.DEFAULT.builder();
        String outputFileName = Paths.get(EXP2P8_RESULT_STORED_PATH, fileName).toString();
        try (Writer writer = new FileWriter(outputFileName, false);
                CSVPrinter csvPrinter = new CSVPrinter(writer, builder.build())) {
            for (List<Integer> tuple : relationInstance) {
                List<Integer> row = tuple.subList(beginColumn, endColumn);
                csvPrinter.printRecord(row);
            }
            System.out.println("result write to: " + outputFileName);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void generateData(Exp2P8CodeGen.GenerateGroup generateGroup)
    {
        List<List<Integer>> a1DotsAkInstance = generateA1DotsAk(generateGroup.numberOfBackJumpedRelations);
        int tupleSize = a1DotsAkInstance.get(0).size();
        checkState(tupleSize == generateGroup.numberOfBackJumpedRelations + 2);
        List<String> fileNames = new ArrayList<>();
        String a1akFileName = String.format("exp2.8%s_%s_%s.csv",
                Joiner.on("").join(IntStream.rangeClosed(1, tupleSize).boxed().collect(Collectors.toList()).stream().map(i -> String.format("a%s", i)).collect(Collectors.toList())),
                generateGroup.numberOfBackJumpedRelations, generateGroup.backjumpedRelationSize);
        fileNames.add(a1akFileName);
        writeCSV(a1akFileName,
                a1DotsAkInstance,
                0,
                tupleSize);
        for (int i = 1; i < tupleSize - 1; ++i) {
            String aiajFileName = String.format("exp2.8a%sa%s_%s_%s.csv", i + 1, i + 2, generateGroup.numberOfBackJumpedRelations, generateGroup.backjumpedRelationSize);
            fileNames.add(aiajFileName);
            List<List<Integer>> relationInstance = new ArrayList<>();
            for (int j = 0; j < generateGroup.backjumpedRelationSize - 1; j++) {
                relationInstance.add(List.of(i + 1, i + 2));
            }
            relationInstance.add(List.of(i + 2, i + 3));
            writeCSV(aiajFileName,
                    relationInstance, 0, 2);
        }
        String akFileName = String.format("exp2.8a%s_%s_%s.csv", tupleSize, generateGroup.numberOfBackJumpedRelations, generateGroup.backjumpedRelationSize);
        writeCSV(akFileName,
                List.of(List.of(tupleSize + 1)),
                0,
                1);
        fileNames.add(akFileName);
        generateGroup.setCsvFiles(fileNames);
    }

    /**
     * Only for testing purpose
     */
    public static void main(String[] args)
    {
        generateData(new Exp2P8CodeGen.GenerateGroup(3, 3));
    }
}
