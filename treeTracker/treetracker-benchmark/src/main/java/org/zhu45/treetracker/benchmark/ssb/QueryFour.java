package org.zhu45.treetracker.benchmark.ssb;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.customerDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.dateDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.lineOrderDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.partDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.ssbSchemaNameDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.supplierDev;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class QueryFour
        extends Query
{
    public QueryFour(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        String ssbSchemaName = isDevMode ? ssbSchemaNameDev : StarSchemaBenchmarkDatabase.ssbSchemaName;
        String lineOrder = isDevMode ? lineOrderDev : StarSchemaBenchmarkDatabase.lineOrder;
        String supplier = isDevMode ? supplierDev : StarSchemaBenchmarkDatabase.supplier;
        String part = isDevMode ? partDev : StarSchemaBenchmarkDatabase.part;
        String customer = isDevMode ? customerDev : StarSchemaBenchmarkDatabase.customer;
        String date = isDevMode ? dateDev : StarSchemaBenchmarkDatabase.date;

        SchemaTableName lineOrderSchema = new SchemaTableName(ssbSchemaName, lineOrder);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup lineorder in SSB database first");
        MultiwayJoinDomain domainLineOrder = new MultiwayJoinDomain();
        MultiwayJoinNode nodeLineOrder = new MultiwayJoinNode(lineOrderSchema, domainLineOrder);

        SchemaTableName partSchema = new SchemaTableName(ssbSchemaName, part);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup part in SSB database first");
        MultiwayJoinDomain domainPart = new MultiwayJoinDomain();
        MultiwayJoinNode nodePart = new MultiwayJoinNode(partSchema, domainPart);

        SchemaTableName customerSchema = new SchemaTableName(ssbSchemaName, customer);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup customer in SSB database first");
        MultiwayJoinDomain domainCustomer = new MultiwayJoinDomain();
        MultiwayJoinNode nodeCustomer = new MultiwayJoinNode(customerSchema, domainCustomer);

        SchemaTableName supplierSchema = new SchemaTableName(ssbSchemaName, supplier);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup supplier in SSB database first");
        MultiwayJoinDomain domainSupplier = new MultiwayJoinDomain();
        MultiwayJoinNode nodeSupplier = new MultiwayJoinNode(supplierSchema, domainSupplier);

        SchemaTableName dateSchema = new SchemaTableName(ssbSchemaName, date);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup date in SSB database first");
        MultiwayJoinDomain domainDate = new MultiwayJoinDomain();
        MultiwayJoinNode nodeDate = new MultiwayJoinNode(dateSchema, domainDate);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeLineOrder, nodePart),
                asEdge(nodeLineOrder, nodeCustomer),
                asEdge(nodeLineOrder, nodeSupplier),
                asEdge(nodeLineOrder, nodeDate))), nodeLineOrder);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(Arrays.asList(lineOrderSchema, partSchema, customerSchema, supplierSchema, dateSchema));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
