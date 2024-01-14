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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.dateDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.lineOrderDev;
import static org.zhu45.treetracker.benchmark.ssb.StarSchemaBenchmarkDatabase.ssbSchemaNameDev;
import static org.zhu45.treetracker.common.Edge.asEdge;

/**
 * Implementation of join fragment of SSB Q1.1
 * <p>
 * select
 * sum(lo_extendedprice * lo_discount) as revenue
 * from
 * LINEORDER,
 * DATE
 * where
 * lo_orderdate = d_datekey
 * and d_year = 1993
 * and lo_discount between 1 and 3
 * and lo_quantity < 25;
 */
public class QueryOne
        extends Query
{
    public QueryOne(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        String ssbSchemaName = isDevMode ? ssbSchemaNameDev : StarSchemaBenchmarkDatabase.ssbSchemaName;
        String lineOrder = isDevMode ? lineOrderDev : StarSchemaBenchmarkDatabase.lineOrder;
        String date = isDevMode ? dateDev : StarSchemaBenchmarkDatabase.date;

        SchemaTableName lineOrderSchema = new SchemaTableName(ssbSchemaName, lineOrder);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup lineorder in SSB database first");
        MultiwayJoinDomain domainLineOrder = new MultiwayJoinDomain();
        MultiwayJoinNode nodeLineOrder = new MultiwayJoinNode(lineOrderSchema, domainLineOrder);

        SchemaTableName dateSchema = new SchemaTableName(ssbSchemaName, date);
        requireNonNull(jdbcClient.getTableHandle(lineOrderSchema), "Need to setup date in SSB database first");
        MultiwayJoinDomain domainDate = new MultiwayJoinDomain();
        MultiwayJoinNode nodeDate = new MultiwayJoinNode(dateSchema, domainDate);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(asEdge(nodeLineOrder, nodeDate)), nodeLineOrder);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(Arrays.asList(lineOrderSchema, dateSchema));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
