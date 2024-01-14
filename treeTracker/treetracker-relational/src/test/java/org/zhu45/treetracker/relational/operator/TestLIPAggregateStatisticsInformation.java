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
import static org.zhu45.treetracker.relational.operator.DatabaseSuppler.TestingMultiwayJoinDatabaseComplexSupplier;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestLIPAggregateStatisticsInformation
{
    private final Logger traceLogger = LogManager.getLogger(TestLIPAggregateStatisticsInformation.class.getName());

    private TestingPhysicalPlanBase base;
    private static String naturalJoinTable = "TestLIPAggregateStatisticsInformation";
    private StatisticsInformationPrinter printer;

    @BeforeAll
    public void setUp()
    {
        TestingMultiwayJoinDatabaseComplex database = TestingMultiwayJoinDatabaseComplexSupplier.get();

        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(LIPAggregateStatisticsInformation.class.getName(), Level.DEBUG);
            Configurator.setAllLevels(TestingPhysicalPlanBase.class.getName(), Level.DEBUG);
        }

        this.base = new TestingPhysicalPlanBase(
                database,
                naturalJoinTable,
                List.of(),
                ExecutionNormal.class,
                Optional.empty(),
                Optional.of(createMap(Optional.of(TupleBasedLIPTableScanOperator.class),
                        Optional.of(TupleBasedLIPHashJoinOperator.class))));
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
                .setJoinOperator(JoinOperator.LIP)
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
        String relationPrefix = "testTwoWayJoinQueryLIP_";
        SchemaTableName schemaTableNameR = new SchemaTableName(schema, relationPrefix + "R");
        SchemaTableName schemaTableNameS = new SchemaTableName(schema, relationPrefix + "S");

        if (base.getDatabase().getJdbcClient().getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationVal = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                relationVal.add(List.of(IntegerValue.of(0), IntegerValue.of(1)));
            }
            for (int i = 0; i < 2; i++) {
                relationVal.add(List.of(IntegerValue.of(1), IntegerValue.of(1)));
            }
            base.getDatabase().getJdbcClient().ingestRelation(
                    schemaTableNameR.getSchemaName(),
                    schemaTableNameR.getTableName(),
                    new ArrayList<>(List.of("a", "b")),
                    new ArrayList<>(List.of(INTEGER, INTEGER)),
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
        // R(a,b) \join S(a), there are 6 tuples in R. 2 R(1,1) are filtered out by Bloom filter from S.
        // Thus, total intermediate results produced is 5 (+1 comes from the last returned null).
        assertEquals(5, aggregateStatisticsInformation.totalIntermediateResultsProduced);
        assertEquals(4, aggregateStatisticsInformation.totalIntermediateResultsProducedWithoutNULL);
        assertEquals(6, aggregateStatisticsInformation.totalInputSizeAfterEvaluation);
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        base.tearDown();
    }
}
