package org.zhu45.treetracker.benchmark.backjump;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.common.Edge;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.backjump.BackJumpDatabase.backjumpSchemaName;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;

public class BackJumpQuery
        extends Query
{
    public BackJumpQuery(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        String multiBackJumpRelationPrefix = "multibackjump_" + backjumpedRelationSize + "_" + numberOfBackJumpedRelations + "_";
        long backjumpedRelationSize = this.backjumpedRelationSize;

        String relationT = multiBackJumpRelationPrefix + "T";
        SchemaTableName schemaTableNameT = new SchemaTableName(backjumpSchemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(List.of(
                    Collections.singletonList(new StringValue(VARCHAR, "red"))));
            jdbcClient.ingestRelation(
                    backjumpSchemaName,
                    relationT,
                    new ArrayList<>(List.of("x")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationS = multiBackJumpRelationPrefix + "S";
        SchemaTableName schemaTableNameS = new SchemaTableName(backjumpSchemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(VARCHAR, "red"), new IntegerValue(1), new IntegerValue(2))));
            jdbcClient.ingestRelation(
                    backjumpSchemaName,
                    relationS,
                    new ArrayList<>(Arrays.asList("x", "y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, INTEGER, INTEGER)),
                    relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationR = multiBackJumpRelationPrefix + "R";
        SchemaTableName schemaTableNameR = new SchemaTableName(backjumpSchemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new IntegerValue(3), new IntegerValue(2))));
            jdbcClient.ingestRelation(
                    backjumpSchemaName,
                    relationR,
                    new ArrayList<>(Arrays.asList("y", "z")),
                    new ArrayList<>(Arrays.asList(INTEGER, INTEGER)),
                    relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        String relationBBase = multiBackJumpRelationPrefix + "B";
        Queue<MultiwayJoinNode> edge = new LinkedList<>();
        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeGroup = new ArrayList<>();
        List<SchemaTableName> tableNames = new ArrayList<>();
        MultiwayJoinNode nodeB0 = null;
        for (int i = 0; i < numberOfBackJumpedRelations; ++i) {
            String relationB = relationBBase + i;
            SchemaTableName schemaTableNameB = new SchemaTableName(backjumpSchemaName, relationB);
            if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
                List<List<RelationalValue>> relationValB = new ArrayList<>();
                for (long j = 0; j < backjumpedRelationSize; j++) {
                    relationValB.add(List.of(new IntegerValue(2)));
                }
                jdbcClient.ingestRelation(
                        backjumpSchemaName,
                        relationB,
                        new ArrayList<>(List.of("z")),
                        new ArrayList<>(List.of(INTEGER)),
                        relationValB);
            }
            MultiwayJoinDomain domainB = new MultiwayJoinDomain();
            MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);
            tableNames.add(schemaTableNameB);
            if (i == 0) {
                nodeB0 = nodeB;
            }
            if (edge.size() < 2) {
                edge.add(nodeB);
            }
            else {
                List<MultiwayJoinNode> edgeTmp = new ArrayList<>(edge);
                edgeGroup.add(asEdge(edgeTmp.get(0), edgeTmp.get(1)));
                edge.poll();
                edge.add(nodeB);
            }
        }
        if (edge.size() == 2) {
            List<MultiwayJoinNode> edgeTmp = new ArrayList<>(edge);
            edgeGroup.add(asEdge(edgeTmp.get(0), edgeTmp.get(1)));
        }

        List<Edge<MultiwayJoinNode, Row, MultiwayJoinDomain>> edgeLists = new ArrayList<>(Arrays.asList(
                asEdge(nodeT, nodeS),
                asEdge(nodeS, nodeB0)));
        edgeLists.addAll(edgeGroup);
        edgeLists.add(asEdge(nodeS, nodeR));

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(edgeLists, nodeT);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        List<SchemaTableName> expectedSchemaTableNamesList = new ArrayList<>(List.of(nodeT.getSchemaTableName(), nodeS.getSchemaTableName()));
        expectedSchemaTableNamesList.addAll(tableNames);
        expectedSchemaTableNamesList.add(nodeR.getSchemaTableName());

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(expectedSchemaTableNamesList);
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));

        return pair;
    }
}
