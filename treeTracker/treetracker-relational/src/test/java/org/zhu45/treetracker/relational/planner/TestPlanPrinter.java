package org.zhu45.treetracker.relational.planner;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.testing.ConnectionLeakUtil;
import org.zhu45.treetracker.jdbc.testing.Database;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.operator.TupleBasedNestedLoopJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;
import org.zhu45.treetracker.relational.planner.printer.PlanPrinter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.zhu45.treetracker.relational.planner.RandomPhysicalPlanBuilder.createMap;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPlanPrinter
{
    ConnectionLeakUtil connectionLeakUtil;

    @BeforeAll
    public void setUp()
    {
        connectionLeakUtil = new ConnectionLeakUtil();
    }

    @Test
    public void testPrintLogicalPlanToText()
    {
        SchemaTableName schemaTableNameR = new SchemaTableName("test", "R");
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        SchemaTableName schemaTableNameT = new SchemaTableName("test", "T");
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        SchemaTableName schemaTableNameS = new SchemaTableName("test", "S");
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        SchemaTableName schemaTableNameU = new SchemaTableName("test", "U");
        MultiwayJoinDomain domainU = new MultiwayJoinDomain();
        MultiwayJoinNode nodeU = new MultiwayJoinNode(schemaTableNameU, domainU);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeR, nodeT),
                asEdge(nodeT, nodeS),
                asEdge(nodeT, nodeU))), nodeR);

        PlanBuildContext context = PlanBuildContext.builder()
                .setOrderedGraph(orderedGraph)
                .setPlanNodeIdAllocator(new PlanNodeIdAllocator())
                .setRules(Collections.emptyList())
                .build();
        PlanBuilder planBuilder = new PlanBuilder(context);
        Plan plan = planBuilder.build();
        PlanPrinter printer = new PlanPrinter(plan.getRoot());
        assertEquals("- [6]Join 4 join 5\n" +
                "    - [4]Join 2 join 3\n" +
                "        - [2]Join 0 join 1\n" +
                "            - [0]Table test.R\n" +
                "            - [1]Table test.T\n" +
                "        - [3]Table test.S\n" +
                "    - [5]Table test.U\n", printer.toText(0));
    }

    @Test
    public void testPrintPhysicalPlanToText()
    {
        List<Operator> operators = null;
        try {
            Database database = new TestingMultiwayJoinDatabase();
            String schemaName = database.getSchemaName();
            JdbcClient jdbcClient = database.getJdbcClient();

            SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, "R");
            MultiwayJoinDomain domainR = new MultiwayJoinDomain();
            MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

            SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, "T");
            MultiwayJoinDomain domainT = new MultiwayJoinDomain();
            MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

            SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, "S");
            MultiwayJoinDomain domainS = new MultiwayJoinDomain();
            MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

            SchemaTableName schemaTableNameU = new SchemaTableName(schemaName, "U");
            MultiwayJoinDomain domainU = new MultiwayJoinDomain();
            MultiwayJoinNode nodeU = new MultiwayJoinNode(schemaTableNameU, domainU);

            MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                    asEdge(nodeR, nodeT),
                    asEdge(nodeT, nodeS),
                    asEdge(nodeT, nodeU))), nodeR);

            PlanBuildContext context = PlanBuildContext.builder()
                    .setOrderedGraph(orderedGraph)
                    .setPlanNodeIdAllocator(new PlanNodeIdAllocator())
                    .setRules(Collections.emptyList())
                    .setOperatorMap(createMap(Optional.of(TupleBasedTableScanOperator.class),
                            Optional.of(TupleBasedNestedLoopJoinOperator.class)))
                    .build();
            PlanBuilder planBuilder = new PlanBuilder(context);
            Plan plan = planBuilder.build();
            context.setJdbcClient(jdbcClient);
            RandomPhysicalPlanBuilder physicalPlanBuilder = new RandomPhysicalPlanBuilder(context);
            Plan physicalPlan = physicalPlanBuilder.build(plan.getRoot());
            operators = physicalPlan.getOperatorList();
            PlanPrinter printer = new PlanPrinter(physicalPlan.getRoot());
            // NOTE: Since we use physicalPlanBuilder build() with default operator implementation list,
            // due to the implementation of visitor to construct the default operator list by picking
            // the first operator implementation of each OptType, this assertion can fail if the first element
            // of a OptType in OperatorMap in physicalPlanBuilder is changed.
            assertEquals(printer.toText(0),
                    "- [6]Join 4 join 5\n" +
                            "        operator = org.zhu45.treetracker.relational.operator.TupleBasedNestedLoopJoinOperator\n" +
                            "    - [4]Join 2 join 3\n" +
                            "            operator = org.zhu45.treetracker.relational.operator.TupleBasedNestedLoopJoinOperator\n" +
                            "        - [2]Join 0 join 1\n" +
                            "                operator = org.zhu45.treetracker.relational.operator.TupleBasedNestedLoopJoinOperator\n" +
                            "            - [0]Table multiway.R\n" +
                            "                    operator = org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator\n" +
                            "            - [1]Table multiway.T\n" +
                            "                    operator = org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator\n" +
                            "        - [3]Table multiway.S\n" +
                            "                operator = org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator\n" +
                            "    - [5]Table multiway.U\n" +
                            "            operator = org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator\n");
        }
        catch (Exception e) {
            throw new TreeTrackerException(GENERIC_INTERNAL_ERROR, e);
        }
        finally {
            operators.forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    ((MultiwayJoinDomain) node.getDomain()).close();
                }
                operator.close();
            });
        }
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        connectionLeakUtil.assertNoLeaks();
    }
}
