package org.zhu45.treektracker.multiwayJoin.testing;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinGraph;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.QueryGraphEdge;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.row.Row;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.jdbc.testing.Database;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.QueryGraphEdge.asQueryGraphEdge;
import static org.zhu45.treetracker.common.type.CharType.createCharType;

public class TestingTreeTrackerJoinComplexCases
{
    private static final CharType charType = createCharType(20);
    private final JdbcClient jdbcClient;
    private final String schemaName;

    public TestingTreeTrackerJoinComplexCases(Database<? extends Row> database)
    {
        this.jdbcClient = database.getJdbcClient();
        this.schemaName = database.getSchemaName();
    }

    /**
     * Generates a query graph that doesn't satisfy label-connectedness (i.e., join tree property):
     * For each attribute, the nodes containing it form a connected sub-graph using only those nodes.
     * TT-2 join should be able to handle the case as well.
     */
    public Pair<MultiwayJoinGraph, MultiwayJoinNode> testTreeTrackerJoinBadCase()
    {
        String relationN = "badcase1_n";
        SchemaTableName schemaTableNameN = new SchemaTableName(schemaName, relationN);
        if (jdbcClient.getTableHandle(schemaTableNameN) == null) {
            List<List<RelationalValue>> relationValN = new ArrayList<>();
            relationValN.add(
                    Arrays.asList(new StringValue(charType, "Navy"), new StringValue(charType, "13")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationN,
                    new ArrayList<>(Arrays.asList("color", "age")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValN);
        }
        MultiwayJoinDomain domainN = new MultiwayJoinDomain();
        MultiwayJoinNode nodeN = new MultiwayJoinNode(schemaTableNameN, domainN);

        String relationU = "badcase1_u";
        SchemaTableName schemaTableNameU = new SchemaTableName(schemaName, relationU);
        if (jdbcClient.getTableHandle(schemaTableNameU) == null) {
            List<List<RelationalValue>> relationValU = new ArrayList<>();
            relationValU.add(
                    Collections.singletonList(new StringValue(charType, "Navy")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationU,
                    new ArrayList<>(Collections.singletonList("color")),
                    new ArrayList<>(Collections.singletonList(charType)),
                    relationValU);
        }
        MultiwayJoinDomain domainU = new MultiwayJoinDomain();
        MultiwayJoinNode nodeU = new MultiwayJoinNode(schemaTableNameU, domainU);

        String relationE = "badcase1_e";
        SchemaTableName schemaTableNameE = new SchemaTableName(schemaName, relationE);
        if (jdbcClient.getTableHandle(schemaTableNameE) == null) {
            List<List<RelationalValue>> relationValE = new ArrayList<>();
            relationValE.add(
                    Arrays.asList(new StringValue(charType, "Navy"), new StringValue(charType, "17")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationE,
                    new ArrayList<>(Arrays.asList("color", "age")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValE);
        }
        MultiwayJoinDomain domainE = new MultiwayJoinDomain();
        MultiwayJoinNode nodeE = new MultiwayJoinNode(schemaTableNameE, domainE);

        List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                asQueryGraphEdge(nodeU, nodeN),
                asQueryGraphEdge(nodeU, nodeE)));
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return Pair.of(g, nodeU);
    }

    /**
     * Generate a graph that tests try-good-list(i) implementation. In specific, it tests
     * - If rest of nodes are skippable, try-good-list should reset iterator of the last node (i.e., last node should not
     * be skipped even if it is skippable)
     * - BaseNode i should be part of skipped if it is skippable
     * - When there is more than skipped nodes when generating the join result, all good values of those skipped nodes
     * should be considered (i.e., a cross product of good values of skipped nodes should be computed and
     * concatenate with tuples from other nodes)
     * <p>
     * exposed by:
     * - seed_db: 8012207984044666129L
     * - seed_graph: 3216267610018358159L
     */
    public Pair<MultiwayJoinGraph, MultiwayJoinNode> testTreeTrackerJoinCaseOne()
    {
        String relationV = "case1_v";
        SchemaTableName schemaTableNameV = new SchemaTableName(schemaName, relationV);
        if (jdbcClient.getTableHandle(schemaTableNameV) == null) {
            List<List<RelationalValue>> relationValV = List.of(
                    Collections.singletonList(new StringValue(charType, "trans")),
                    Collections.singletonList(new StringValue(charType, "trans")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationV,
                    new ArrayList<>(Collections.singletonList("sex")),
                    new ArrayList<>(Collections.singletonList(charType)),
                    relationValV);
        }
        MultiwayJoinDomain domainV = new MultiwayJoinDomain();
        MultiwayJoinNode nodeV = new MultiwayJoinNode(schemaTableNameV, domainV);

        String relationY = "case1_y";
        SchemaTableName schemaTableNameY = new SchemaTableName(schemaName, relationY);
        if (jdbcClient.getTableHandle(schemaTableNameY) == null) {
            List<List<RelationalValue>> relationValY = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(charType, "Emma"), new StringValue(charType, "trans")),
                    Arrays.asList(new StringValue(charType, "Benjamin"), new StringValue(charType, "trans"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationY,
                    new ArrayList<>(Arrays.asList("name", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValY);
        }
        MultiwayJoinDomain domainY = new MultiwayJoinDomain();
        MultiwayJoinNode nodeY = new MultiwayJoinNode(schemaTableNameY, domainY);

        String relationVQ = "case1_vq";
        SchemaTableName schemaTableNameVQ = new SchemaTableName(schemaName, relationVQ);
        if (jdbcClient.getTableHandle(schemaTableNameVQ) == null) {
            List<List<RelationalValue>> relationValVQ = List.of(
                    Arrays.asList(new StringValue(charType, "11"), new StringValue(charType, "trans")),
                    Arrays.asList(new StringValue(charType, "12"), new StringValue(charType, "trans")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationVQ,
                    new ArrayList<>(Arrays.asList("age", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValVQ);
        }
        MultiwayJoinDomain domainVQ = new MultiwayJoinDomain();
        MultiwayJoinNode nodeVQ = new MultiwayJoinNode(schemaTableNameVQ, domainVQ);

        List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                asQueryGraphEdge(nodeV, nodeY),
                asQueryGraphEdge(nodeY, nodeVQ)));
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return Pair.of(g, nodeV);
    }

    /**
     * Generate a graph that tests try-good-list(i) implementation. The graph node C
     * is not skippable whereas node VQ should be skipped. This test case also shares the same testing purpose
     * as testTreeTrackerJoinCaseOne.
     * <p>
     * exposed by:
     * - seed_db: 8012207984044666129L
     * - seed_graph: 3216267610018358159L
     */
    public Pair<MultiwayJoinGraph, MultiwayJoinNode> testTreeTrackerJoinCaseTwo()
    {
        String relationV = "case2_v";
        SchemaTableName schemaTableNameV = new SchemaTableName(schemaName, relationV);
        if (jdbcClient.getTableHandle(schemaTableNameV) == null) {
            List<List<RelationalValue>> relationValV = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue(charType, "trans")),
                    Collections.singletonList(new StringValue(charType, "trans"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationV,
                    new ArrayList<>(Collections.singletonList("sex")),
                    new ArrayList<>(Collections.singletonList(charType)),
                    relationValV);
        }
        MultiwayJoinDomain domainV = new MultiwayJoinDomain();
        MultiwayJoinNode nodeV = new MultiwayJoinNode(schemaTableNameV, domainV);

        String relationY = "case2_y";
        SchemaTableName schemaTableNameY = new SchemaTableName(schemaName, relationY);
        if (jdbcClient.getTableHandle(schemaTableNameY) == null) {
            List<List<RelationalValue>> relationValY = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(charType, "Emma"), new StringValue(charType, "trans")),
                    Arrays.asList(new StringValue(charType, "Benjamin"), new StringValue(charType, "trans"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationY,
                    new ArrayList<>(Arrays.asList("name", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValY);
        }
        MultiwayJoinDomain domainY = new MultiwayJoinDomain();
        MultiwayJoinNode nodeY = new MultiwayJoinNode(schemaTableNameY, domainY);

        String relationVQ = "case2_vq";
        SchemaTableName schemaTableNameVQ = new SchemaTableName(schemaName, relationVQ);
        if (jdbcClient.getTableHandle(schemaTableNameVQ) == null) {
            List<List<RelationalValue>> relationValVQ = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(charType, "11"), new StringValue(charType, "trans")),
                    Arrays.asList(new StringValue(charType, "12"), new StringValue(charType, "trans"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationVQ,
                    new ArrayList<>(Arrays.asList("age", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValVQ);
        }
        MultiwayJoinDomain domainVQ = new MultiwayJoinDomain();
        MultiwayJoinNode nodeVQ = new MultiwayJoinNode(schemaTableNameVQ, domainVQ);

        String relationC = "case2_c";
        SchemaTableName schemaTableNameC = new SchemaTableName(schemaName, relationC);
        if (jdbcClient.getTableHandle(schemaTableNameC) == null) {
            List<List<RelationalValue>> relationValC = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(charType, "trans"), new StringValue(charType, "red")),
                    Arrays.asList(new StringValue(charType, "trans"), new StringValue(charType, "yellow"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationC,
                    new ArrayList<>(Arrays.asList("sex", "color")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValC);
        }
        MultiwayJoinDomain domainC = new MultiwayJoinDomain();
        MultiwayJoinNode nodeC = new MultiwayJoinNode(schemaTableNameC, domainC);

        List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                asQueryGraphEdge(nodeV, nodeY),
                asQueryGraphEdge(nodeY, nodeVQ),
                asQueryGraphEdge(nodeV, nodeC)));
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return Pair.of(g, nodeV);
    }

    /**
     * This graph tests the following scenario:
     * "blue" of node B is already in its good-list when the first joined tuple: (13, trans, blue) is discovered.
     * TT-2 join continues to run and at some point, node D will be instantiated with "(15, male)" and the node B's
     * iterator will point to "turquoise". Then once the execution reaches node C, backtrack happens and the execution
     * backs to node D. Then, node D will be instantiated with "(6, trans)". Then, due to try-good-list, good value
     * of node O (e.g., "(blue, trans)") and node B ("blue") can be used directly to work with "(6, trans)". Then,
     * node C is iterated from the beginning to find the next joined tuple: (6, trans, blue). Execution continues.
     * Now, here is the problem: since node C has no more values, node B will be examined. Since node B's iterator pointing
     * to "turquoise", the next value is "blue". We have problem because duplicate joined tuples will be generated because
     * "blue" is effectively checked during try-good-list and cannot be used again.
     * <p>
     * exposed by:
     * - seed_db: 221632311526772538L
     * - seed_graph: 6472814709492076677L
     */
    public Pair<MultiwayJoinGraph, MultiwayJoinNode> testTreeTrackerJoinCaseThree()
    {
        String relationD = "case3_d";
        SchemaTableName schemaTableNameD = new SchemaTableName(schemaName, relationD);
        if (jdbcClient.getTableHandle(schemaTableNameD) == null) {
            List<List<RelationalValue>> relationValD = List.of(
                    Arrays.asList(new StringValue(charType, "13"), new StringValue(charType, "trans")),
                    Arrays.asList(new StringValue(charType, "15"), new StringValue(charType, "male")),
                    Arrays.asList(new StringValue(charType, "6"), new StringValue(charType, "trans")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationD,
                    new ArrayList<>(Arrays.asList("age", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValD);
        }
        MultiwayJoinDomain domainD = new MultiwayJoinDomain();
        MultiwayJoinNode nodeD = new MultiwayJoinNode(schemaTableNameD, domainD);

        String relationO = "case3_o";
        SchemaTableName schemaTableNameO = new SchemaTableName(schemaName, relationO);
        if (jdbcClient.getTableHandle(schemaTableNameO) == null) {
            List<List<RelationalValue>> relationValO = List.of(
                    Arrays.asList(new StringValue(charType, "turquoise"), new StringValue(charType, "male")),
                    Arrays.asList(new StringValue(charType, "blue"), new StringValue(charType, "trans")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationO,
                    new ArrayList<>(Arrays.asList("color", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValO);
        }
        MultiwayJoinDomain domainO = new MultiwayJoinDomain();
        MultiwayJoinNode nodeO = new MultiwayJoinNode(schemaTableNameO, domainO);

        String relationB = "case3_b";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = List.of(
                    List.of(new StringValue(charType, "turquoise")),
                    List.of(new StringValue(charType, "blue")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationB,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        String relationC = "case3_c";
        SchemaTableName schemaTableNameC = new SchemaTableName(schemaName, relationC);
        if (jdbcClient.getTableHandle(schemaTableNameC) == null) {
            List<List<RelationalValue>> relationValC = new ArrayList<>(List.of(
                    Arrays.asList(new StringValue(charType, "trans"), new StringValue(charType, "blue"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationC,
                    new ArrayList<>(Arrays.asList("sex", "color")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValC);
        }
        MultiwayJoinDomain domainC = new MultiwayJoinDomain();
        MultiwayJoinNode nodeC = new MultiwayJoinNode(schemaTableNameC, domainC);

        List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                asQueryGraphEdge(nodeD, nodeO),
                asQueryGraphEdge(nodeD, nodeC),
                asQueryGraphEdge(nodeO, nodeB)));
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return Pair.of(g, nodeD);
    }

    /**
     * Test implementation on isInstantiate() of MultiwayJoinConstraints. In specific, during generateJoinTuples(),
     * skipped nodes assigned value should not set to empty by isInstantiate() because this may lead to the situation
     * in future TT-2 algorithm execution in which some node's parent node has empty assigned value. This can cause
     * bad value goes into good list, which breaks goodlist-semijoin reduction invariant.
     * <p>
     * exposed by:
     * - seed_db: 221632311526772538L
     * - seed_graph: 6472814709492076677L
     */
    public Pair<MultiwayJoinGraph, MultiwayJoinNode> testTreeTrackerJoinCaseFour()
    {
        String relationD = "case4_d";
        SchemaTableName schemaTableNameD = new SchemaTableName(schemaName, relationD);
        if (jdbcClient.getTableHandle(schemaTableNameD) == null) {
            List<List<RelationalValue>> relationValD = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(charType, "13"), new StringValue(charType, "trans")),
                    Arrays.asList(new StringValue(charType, "15"), new StringValue(charType, "male")),
                    Arrays.asList(new StringValue(charType, "6"), new StringValue(charType, "trans"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationD,
                    new ArrayList<>(Arrays.asList("age", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValD);
        }
        MultiwayJoinDomain domainD = new MultiwayJoinDomain();
        MultiwayJoinNode nodeD = new MultiwayJoinNode(schemaTableNameD, domainD);

        String relationO = "case4_o";
        SchemaTableName schemaTableNameO = new SchemaTableName(schemaName, relationO);
        if (jdbcClient.getTableHandle(schemaTableNameO) == null) {
            List<List<RelationalValue>> relationValO = new ArrayList<>(Arrays.asList(
                    Arrays.asList(new StringValue(charType, "turquoise"), new StringValue(charType, "male")),
                    Arrays.asList(new StringValue(charType, "blue"), new StringValue(charType, "trans"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationO,
                    new ArrayList<>(Arrays.asList("color", "sex")),
                    new ArrayList<>(Arrays.asList(charType, charType)),
                    relationValO);
        }
        MultiwayJoinDomain domainO = new MultiwayJoinDomain();
        MultiwayJoinNode nodeO = new MultiwayJoinNode(schemaTableNameO, domainO);

        String relationB = "case4_b";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = List.of(
                    List.of(new StringValue(charType, "turquoise")),
                    List.of(new StringValue(charType, "black")),
                    List.of(new StringValue(charType, "purple")),
                    List.of(new StringValue(charType, "blue")),
                    List.of(new StringValue(charType, "black")));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationB,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                asQueryGraphEdge(nodeD, nodeO),
                asQueryGraphEdge(nodeO, nodeB)));
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return Pair.of(g, nodeD);
    }

    /**
     * A test case from ttj.tex:
     * Suppose D = {B(a), A(a), S(a), R(a)} and
     * I(D) = {B(``red"), B(``red"), A(``red"), S(``red"), R(``red")}.
     * The query graph is B -> A -> S -> R$.
     * TT-2 Join will backtrack to B once the first joined tuple ("red") is found.
     * Once node B is instantiated with the second ("red"), try-good-list is called. Since
     * node A, node S, node R belong to the same subtree, the algorithm should be done without further
     * checking S and R. This test case tests implementation of TT-2 Join should
     * skip node S and node R.
     */
    public Pair<MultiwayJoinGraph, MultiwayJoinNode> testTreeTrackerJoinCaseFive()
    {
        String relationB = "case5_b";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                    List.of(new StringValue(charType, "red")),
                    List.of(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationB,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        String relationA = "case5_a";
        SchemaTableName schemaTableNameA = new SchemaTableName(schemaName, relationA);
        if (jdbcClient.getTableHandle(schemaTableNameA) == null) {
            List<List<RelationalValue>> relationValA = new ArrayList<>(List.of(
                    List.of(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationA,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValA);
        }
        MultiwayJoinDomain domainA = new MultiwayJoinDomain();
        MultiwayJoinNode nodeA = new MultiwayJoinNode(schemaTableNameA, domainA);

        String relationS = "case5_s";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(List.of(
                    List.of(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationS,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationR = "case5_r";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(
                    List.of(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationR,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        List<QueryGraphEdge> edgeLists = new ArrayList<>(Arrays.asList(
                asQueryGraphEdge(nodeB, nodeA),
                asQueryGraphEdge(nodeA, nodeS),
                asQueryGraphEdge(nodeS, nodeR)));
        MultiwayJoinGraph g = new MultiwayJoinGraph(edgeLists);
        return Pair.of(g, nodeB);
    }
}
