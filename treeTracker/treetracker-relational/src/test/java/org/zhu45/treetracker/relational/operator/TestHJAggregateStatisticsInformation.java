package org.zhu45.treetracker.relational.operator;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabaseComplex;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;
import org.zhu45.treetracker.relational.planner.rule.JoinOrdering;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHJAggregateStatisticsInformation
{
    private Logger traceLogger = LogManager.getLogger(TestHJAggregateStatisticsInformation.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestHJAggregateStatisticsInformation";
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(AggregateStatisticsInformation.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.DEBUG);
        }

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedTableScanOperator.class),
                        Optional.of(TupleBasedHashJoinOperator.class))));
        printer = new StatisticsInformationPrinter();
    }

    private AggregateStatisticsInformation testStatistics(Pair<Plan, List<Operator>> pair)
    {
        base.testPhysicalPlanExecution(Pair.of(pair.getLeft(), pair.getRight()));
        Operator rootOperator = pair.getLeft().getRoot().getOperator();
        String statistics = printer.print(rootOperator);
        if (traceLogger.isDebugEnabled()) {
            traceLogger.debug("statistics:\n" + statistics);
        }
        AggregateStatisticsInformationContext aggregateStatisticsInformationContext = AggregateStatisticsInformationContext.builder()
                .setRootOperator(rootOperator)
                .setJoinOperator(JoinOperator.HASH_JOIN)
                .build();
        AggregateStatisticsInformationFactory factory = new AggregateStatisticsInformationFactory(aggregateStatisticsInformationContext);
        return factory.get();
    }

    /**
     * Inspired from TwoWayJoinQuery from Microbench
     */
    @Test
    public void testTwoWayJoinQuery()
    {
        String schema = base.getDatabase().getSchemaName();
        String relationPrefix = "testTwoWayJoinQuery_";
        SchemaTableName schemaTableNameR = new SchemaTableName(schema, relationPrefix + "R");
        SchemaTableName schemaTableNameS = new SchemaTableName(schema, relationPrefix + "S");

        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>();
            for (int i = 0; i < 4000; i++) {
                relationVal.add(List.of(IntegerValue.of(0)));
            }
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameR.getSchemaName(),
                    schemaTableNameR.getTableName(),
                    new ArrayList<>(List.of("a")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(0)),
                    List.of(IntegerValue.of(0))));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameS.getSchemaName(),
                    schemaTableNameS.getTableName(),
                    new ArrayList<>(List.of("a")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromJoinOrdering(new JoinOrdering(List.of(schemaTableNameR, schemaTableNameS)));
        AggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(pair);
        // R \join S, there are 4000 tuples in R (+1 comes from the last returned null). Thus, total intermediate
        // results produced is 4001.
        assertEquals(4001, aggregateStatisticsInformation.totalIntermediateResultsProduced);
        assertEquals(4000, aggregateStatisticsInformation.numberOfHashTableProbe);
    }

    /**
     * Chain query example from cost-model3.pdf
     */
    @Test
    public void testChainQuery()
    {
        String schema = base.getDatabase().getSchemaName();
        String relationPrefix = "testChainQuery_";
        SchemaTableName schemaTableNameC = new SchemaTableName(schema, relationPrefix + "C");
        SchemaTableName schemaTableNameR = new SchemaTableName(schema, relationPrefix + "R");
        SchemaTableName schemaTableNameB = new SchemaTableName(schema, relationPrefix + "B");
        SchemaTableName schemaTableNameG = new SchemaTableName(schema, relationPrefix + "G");

        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameC) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(0)),
                    List.of(IntegerValue.of(0)),
                    List.of(IntegerValue.of(0)),
                    List.of(IntegerValue.of(0)),
                    List.of(IntegerValue.of(3)),
                    List.of(IntegerValue.of(6)),
                    List.of(IntegerValue.of(5))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameC.getSchemaName(),
                    schemaTableNameC.getTableName(),
                    new ArrayList<>(List.of("y")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(2), IntegerValue.of(3)),
                    List.of(IntegerValue.of(2), IntegerValue.of(3)),
                    List.of(IntegerValue.of(2), IntegerValue.of(3)),
                    List.of(IntegerValue.of(2), IntegerValue.of(6)),
                    List.of(IntegerValue.of(2), IntegerValue.of(5))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameR.getSchemaName(),
                    schemaTableNameR.getTableName(),
                    new ArrayList<>(List.of("x", "y")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(2), IntegerValue.of(6)),
                    List.of(IntegerValue.of(2), IntegerValue.of(5))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameB.getSchemaName(),
                    schemaTableNameB.getTableName(),
                    new ArrayList<>(List.of("x", "y")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameG) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(5)),
                    List.of(IntegerValue.of(5))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameG.getSchemaName(),
                    schemaTableNameG.getTableName(),
                    new ArrayList<>(List.of("y")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromJoinOrdering(
                new JoinOrdering(List.of(schemaTableNameC,
                        schemaTableNameR, schemaTableNameB, schemaTableNameG)));
        AggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(pair);
        // C \join2 R \join4 B \join6 G. 8 tuples from C (+1 due to null). \join2 produces 6 intermediate
        // results (+1 due to null). \join4 produces 3 intermediate results (+1 due to null). Thus,
        // we have 8 + 6 + 3 = 17.
        assertEquals(17, aggregateStatisticsInformation.totalIntermediateResultsProduced);
        // number of hash table probe from C is 7, which equals to |C|. For each join result from C \join R,
        // it will probe hash table on B, which in total is 5. For each join result from C \join R \join B, it will probe
        // G, which in total is 2. So, to sum up, 7 + 5 + 2 = 14.
        assertEquals(14, aggregateStatisticsInformation.getNumberOfHashTableProbe());
        assertEquals(aggregateStatisticsInformation.getNumberOfHashTableProbe(),
                aggregateStatisticsInformation.getTotalIntermediateResultsProducedWithoutNULL());
    }

    /**
     * Example 2 in cost-model3.pdf (Paper example query)
     */
    @Test
    public void testPaperExampleQuery()
    {
        String schema = base.getDatabase().getSchemaName();
        String relationPrefix = "testPaperExampleQuery_";
        SchemaTableName schemaTableNameT = new SchemaTableName(schema, relationPrefix + "T");
        SchemaTableName schemaTableNameS = new SchemaTableName(schema, relationPrefix + "S");
        SchemaTableName schemaTableNameB = new SchemaTableName(schema, relationPrefix + "B");
        SchemaTableName schemaTableNameR = new SchemaTableName(schema, relationPrefix + "R");

        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(StringValue.of("green")),
                    List.of(StringValue.of("green")),
                    List.of(StringValue.of("red"))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameT.getSchemaName(),
                    schemaTableNameT.getTableName(),
                    new ArrayList<>(List.of("x")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(StringValue.of("red"), IntegerValue.of(1), IntegerValue.of(2)),
                    List.of(StringValue.of("red"), IntegerValue.of(3), IntegerValue.of(2))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameS.getSchemaName(),
                    schemaTableNameS.getTableName(),
                    new ArrayList<>(List.of("x", "y", "z")),
                    new ArrayList<>(List.of(VARCHAR, INTEGER, INTEGER)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(2))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameB.getSchemaName(),
                    schemaTableNameB.getTableName(),
                    new ArrayList<>(List.of("z")),
                    new ArrayList<>(List.of(INTEGER)),
                    relationVal);
        }
        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>(Arrays.asList(
                    List.of(IntegerValue.of(3), IntegerValue.of(2))
            ));
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameR.getSchemaName(),
                    schemaTableNameR.getTableName(),
                    new ArrayList<>(List.of("y", "z")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
                    relationVal);
        }
        Pair<Plan, List<Operator>> pair = base.createPhysicalPlanFromJoinOrdering(
                new JoinOrdering(List.of(schemaTableNameT,
                        schemaTableNameS, schemaTableNameB, schemaTableNameR)));
        AggregateStatisticsInformation aggregateStatisticsInformation = testStatistics(pair);
        // T \join2 S \join4 B \join6 R. T produces 4 intermediate results (+1 due to return null).
        // \Join2 produces 3 intermediate results (+1 due to return null). \Join4 produces 3 intermediate
        // results (+1 due to null). Thus, in total, 4 + 3 + 3 = 10.
        assertEquals(10, aggregateStatisticsInformation.totalIntermediateResultsProduced);
        assertEquals(7, aggregateStatisticsInformation.numberOfHashTableProbe);
        assertEquals(aggregateStatisticsInformation.numberOfHashTableProbe,
                aggregateStatisticsInformation.getTotalIntermediateResultsProducedWithoutNULL());
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
