package org.zhu45.treetracker.benchmark.micro;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.micro.MicroBenchDatabase.microbenchSchemaName;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;

public class PaperExampleQuery1
        extends Query
{
    public PaperExampleQuery1(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        String relationT = "PEQ_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(microbenchSchemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue(VARCHAR, "red"))));
            jdbcClient.ingestRelation(
                    microbenchSchemaName,
                    relationT,
                    new ArrayList<>(List.of("x")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValT);
        }
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, new MultiwayJoinDomain());

        String relationS = "PEQ_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(microbenchSchemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                    List.of(new StringValue(VARCHAR, "red"),
                            new StringValue(VARCHAR, "1"),
                            new StringValue(VARCHAR, "2")),
                    Arrays.asList(new StringValue(VARCHAR, "red"),
                            new StringValue(VARCHAR, "3"),
                            new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(
                    microbenchSchemaName,
                    relationS,
                    new ArrayList<>(Arrays.asList("x", "y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)),
                    relationValS);
        }
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, new MultiwayJoinDomain());

        String relationG = "PEQ_G";
        SchemaTableName schemaTableNameG = new SchemaTableName(microbenchSchemaName, relationG);
        if (jdbcClient.getTableHandle(schemaTableNameG) == null) {
            List<List<RelationalValue>> relationValG = new ArrayList<>(1024);
            for (int i = 0; i < 1024; i++) {
                relationValG.add(Collections.singletonList(new StringValue(VARCHAR, "2")));
            }
            jdbcClient.ingestRelation(
                    microbenchSchemaName,
                    relationG,
                    new ArrayList<>(List.of("z")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValG);
        }
        MultiwayJoinNode nodeG = new MultiwayJoinNode(schemaTableNameG, new MultiwayJoinDomain());

        String relationB = "PEQ_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(microbenchSchemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(1024);
            for (int i = 0; i < 1024; i++) {
                relationValB.add(Collections.singletonList(new StringValue(VARCHAR, "2")));
            }
            jdbcClient.ingestRelation(
                    microbenchSchemaName,
                    relationB,
                    new ArrayList<>(List.of("z")),
                    new ArrayList<>(List.of(VARCHAR)),
                    relationValB);
        }
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, new MultiwayJoinDomain());

        String relationR = "PEQ_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(microbenchSchemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(
                    Arrays.asList(new StringValue(VARCHAR, "3"),
                            new StringValue(VARCHAR, "2"))));
            jdbcClient.ingestRelation(
                    microbenchSchemaName,
                    relationR,
                    new ArrayList<>(Arrays.asList("y", "z")),
                    new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)),
                    relationValR);
        }
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(nodeT, nodeS),
                asEdge(nodeS, nodeG),
                asEdge(nodeG, nodeB),
                asEdge(nodeS, nodeR)), nodeT);

        return createFixedPhysicalPlanFromQueryGraph(orderedGraph);
    }
}
