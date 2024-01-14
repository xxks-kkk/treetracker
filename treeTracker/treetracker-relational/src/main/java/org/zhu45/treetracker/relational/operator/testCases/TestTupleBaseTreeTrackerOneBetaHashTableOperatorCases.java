package org.zhu45.treetracker.relational.operator.testCases;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.IntegerValue;
import org.zhu45.treetracker.common.RelationalValue;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.common.StringValue;
import org.zhu45.treetracker.common.type.CharType;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;
import org.zhu45.treetracker.relational.planner.TestingPhysicalPlanBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.common.Edge.asEdge;
import static org.zhu45.treetracker.common.type.CharType.createCharType;
import static org.zhu45.treetracker.common.type.IntegerType.INTEGER;
import static org.zhu45.treetracker.common.type.VarcharType.VARCHAR;

public class TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases
        implements TestCases
{
    private static CharType charType = createCharType(20);
    private String schemaName;
    private JdbcClient jdbcClient;
    private TestingPhysicalPlanBase base;

    public TestTupleBaseTreeTrackerOneBetaHashTableOperatorCases(TestingPhysicalPlanBase base)
    {
        this.schemaName = base.getDatabase().getSchemaName();
        this.jdbcClient = base.getDatabase().getJdbcClient();
        this.base = base;
    }

    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseOne()
    {
        String relationT = "caseone_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue("red")), Collections.singletonList(new StringValue("black")), Collections.singletonList(new StringValue("yellow"))));
            jdbcClient.ingestRelation(schemaName, relationT, new ArrayList<>(List.of("color")), new ArrayList<>(List.of(VARCHAR)), relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationS = "caseone_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue("red")), Collections.singletonList(new StringValue("black")), Collections.singletonList(new StringValue("yellow"))));
            jdbcClient.ingestRelation(schemaName, relationS, new ArrayList<>(List.of("color")), new ArrayList<>(List.of(VARCHAR)), relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationR = "caseone_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue("green")), Collections.singletonList(new StringValue("black")), Collections.singletonList(new StringValue("yellow"))));
            jdbcClient.ingestRelation(schemaName, relationR, new ArrayList<>(List.of("color")), new ArrayList<>(List.of(VARCHAR)), relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        String relationB = "caseone_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue("yellow")), Collections.singletonList(new StringValue("yellow"))));
            jdbcClient.ingestRelation(schemaName, relationB, new ArrayList<>(List.of("color")), new ArrayList<>(List.of(VARCHAR)), relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeR), asEdge(nodeS, nodeB))), nodeT);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameR, schemaTableNameB))), Optional.empty());
    }

    /**
     * Show why we cannot remove the entire entry of H based on jav.
     */
    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseTwo()
    {
        String relationT = "casetwo_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(List.of(Collections.singletonList(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(schemaName, relationT, new ArrayList<>(List.of("color")), new ArrayList<>(List.of(charType)), relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationS = "casetwo_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(Arrays.asList(new StringValue(charType, "red"), new StringValue(charType, "b"), new StringValue(charType, "c")), Arrays.asList(new StringValue(charType, "red"), new StringValue(charType, "a"), new StringValue(charType, "c"))));
            jdbcClient.ingestRelation(schemaName, relationS, new ArrayList<>(Arrays.asList("color", "letter1", "letter2")), new ArrayList<>(Arrays.asList(charType, charType, charType)), relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationR = "casetwo_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(Arrays.asList(new StringValue(charType, "a"), new StringValue(charType, "c"))));
            jdbcClient.ingestRelation(schemaName, relationR, new ArrayList<>(Arrays.asList("letter1", "letter2")), new ArrayList<>(Arrays.asList(charType, charType)), relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        String relationB = "casetwo_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(Collections.singletonList(new StringValue(charType, "a"))));
            jdbcClient.ingestRelation(schemaName, relationB, new ArrayList<>(List.of("letter1")), new ArrayList<>(List.of(charType)), relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeR), asEdge(nodeS, nodeB))), nodeT);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameR, schemaTableNameB))), Optional.empty());
    }

    /**
     * This test case motivates why when getNewR1 == false, we want to clear res. Details see ttj.tex.
     * Exposed by seed: -3198790133832417557L for TestTupleBaseTreeTrackerOneBetaHashTableOperator.
     */
    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseThree()
    {
        String relationCB = "casethree_CB";
        SchemaTableName schemaTableNameCB = new SchemaTableName(schemaName, relationCB);
        if (jdbcClient.getTableHandle(schemaTableNameCB) == null) {
            List<List<RelationalValue>> relationValCB = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(charType, "female")), Collections.singletonList(new StringValue(charType, "male")), Collections.singletonList(new StringValue(charType, "male"))));
            jdbcClient.ingestRelation(schemaName, relationCB, new ArrayList<>(Arrays.asList("sex")), new ArrayList<>(Arrays.asList(charType)), relationValCB);
        }
        MultiwayJoinDomain domainCB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeCB = new MultiwayJoinNode(schemaTableNameCB, domainCB);

        String relationEU = "casethree_EU";
        SchemaTableName schemaTableNameEU = new SchemaTableName(schemaName, relationEU);
        if (jdbcClient.getTableHandle(schemaTableNameEU) == null) {
            List<List<RelationalValue>> relationValEU = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(charType, "female")), Collections.singletonList(new StringValue(charType, "male")), Collections.singletonList(new StringValue(charType, "male"))));
            jdbcClient.ingestRelation(schemaName, relationEU, new ArrayList<>(Arrays.asList("sex")), new ArrayList<>(Arrays.asList(charType)), relationValEU);
        }
        MultiwayJoinDomain domainEU = new MultiwayJoinDomain();
        MultiwayJoinNode nodeEU = new MultiwayJoinNode(schemaTableNameEU, domainEU);

        String relationJ = "casethree_J";
        SchemaTableName schemaTableNameJ = new SchemaTableName(schemaName, relationJ);
        if (jdbcClient.getTableHandle(schemaTableNameJ) == null) {
            List<List<RelationalValue>> relationValJ = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(charType, "female")), Collections.singletonList(new StringValue(charType, "female")), Collections.singletonList(new StringValue(charType, "female")), Collections.singletonList(new StringValue(charType, "male"))));
            jdbcClient.ingestRelation(schemaName, relationJ, new ArrayList<>(Arrays.asList("sex")), new ArrayList<>(Arrays.asList(charType)), relationValJ);
        }
        MultiwayJoinDomain domainJ = new MultiwayJoinDomain();
        MultiwayJoinNode nodeJ = new MultiwayJoinNode(schemaTableNameJ, domainJ);

        String relationQ = "casethree_Q";
        SchemaTableName schemaTableNameQ = new SchemaTableName(schemaName, relationQ);
        if (jdbcClient.getTableHandle(schemaTableNameQ) == null) {
            List<List<RelationalValue>> relationValQ = new ArrayList<>(List.of(Collections.singletonList(new StringValue(charType, "male"))));
            jdbcClient.ingestRelation(schemaName, relationQ, new ArrayList<>(List.of("sex")), new ArrayList<>(List.of(charType)), relationValQ);
        }
        MultiwayJoinDomain domainQ = new MultiwayJoinDomain();
        MultiwayJoinNode nodeQ = new MultiwayJoinNode(schemaTableNameQ, domainQ);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeCB, nodeEU), asEdge(nodeEU, nodeJ), asEdge(nodeEU, nodeQ))), nodeCB);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameCB, schemaTableNameEU, schemaTableNameJ, schemaTableNameQ))), Optional.empty());
    }

    /**
     * This test case tests out that getR2(false) that calls LookUpH() may have duplicate return. In this case,
     * we have D = {T(x), S(x,y,z), B(z), R(y,z)} and I = {T(red), S(red,3,2), S(red,1,2), B(2), R(3,2)}.
     * The query plan is (join1 (join2 (join3 T S) B) R). Query graph see test case. Error scenario is following:
     * when (red,1,2) returned as join1.r1, backtrack happens.
     * PassContext(R) is recursively called until join3.PassContext(R), where S is the parent of R. H_S is updated and
     * GetR2(false) of join3 is called, which further calls LookUpH().
     * In the previous implementation (e.g., commit id: 80dea86cfe93d4824b5f7511e0203c9759b1ca6b), l is directly computed.
     * Then, it is likely that the duplicate tuple will create. In this case, even though (red,1,2) is removed, l = [(red,3,2)].
     * And, another (red,3,2) join result is generated. However, (red,3,2) is already generated. Thus, the result size
     * is 2 (two (red,3,2) tuples) instead of 1 (one (red,3,2)), which is expected.
     */
    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFour()
    {
        String relationT = "casefour_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        List<String> attributesT = new ArrayList<>(List.of("x"));
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue(charType, "green")), Collections.singletonList(new StringValue(charType, "green")), Collections.singletonList(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(schemaName, relationT, attributesT, new ArrayList<>(List.of(charType)), relationValT);
        }
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, attributesT, new MultiwayJoinDomain());

        String relationS = "casefour_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        List<String> attributeS = new ArrayList<>(Arrays.asList("x", "y", "z"));
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(List.of(new StringValue(charType, "red"), new StringValue(charType, "1"), new StringValue(charType, "2")), Arrays.asList(new StringValue(charType, "red"), new StringValue(charType, "3"), new StringValue(charType, "2"))));
            jdbcClient.ingestRelation(schemaName, relationS, attributeS, new ArrayList<>(Arrays.asList(charType, charType, charType)), relationValS);
        }
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, attributeS, new MultiwayJoinDomain());

        String relationB = "casefour_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        List<String> attributeB = new ArrayList<>(List.of("z"));
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(List.of(Collections.singletonList(new StringValue(charType, "2"))));
            jdbcClient.ingestRelation(schemaName, relationB, attributeB, new ArrayList<>(List.of(charType)), relationValB);
        }
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributeB, new MultiwayJoinDomain());

        String relationR = "casefour_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        List<String> attributeR = new ArrayList<>(Arrays.asList("y", "z"));
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(Arrays.asList(new StringValue(charType, "3"), new StringValue(charType, "2"))));
            jdbcClient.ingestRelation(schemaName, relationR, attributeR, new ArrayList<>(Arrays.asList(charType, charType)), relationValR);
        }
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributeR, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeB), asEdge(nodeS, nodeR))), nodeT);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameB, schemaTableNameR))), Optional.empty());
    }

    /**
     * This test case exposes the issue that TTJ may mark good tuple (tuple that can be part of join result)
     * as no-good due to PassContext() calls. In this example, after all (red,1,2) of S are removed (due to they
     * cannot join with (3,2) of R, PassContext(T) is called and T(red) is marked as no-good. Thus, the 2nd T(red)
     * will be filtered out and thus lead to incorrect join result.
     */
    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFive()
    {
        int numberOfNoGoodTuples = 2;
        int numberOfTuplesRemovedFromHashTableS = 3;

        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "casefive_";
        String relationT = relationNamePrefix + "T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue("green")), Collections.singletonList(new StringValue("red"))));
            for (int i = 0; i < numberOfNoGoodTuples; ++i) {
                relationValT.add(Collections.singletonList(new StringValue("green")));
            }
            relationValT.add(Collections.singletonList(new StringValue("blue")));
            relationValT.add(Collections.singletonList(new StringValue("red")));
            jdbcClient.ingestRelation(schemaName, relationT, new ArrayList<>(List.of("x")), new ArrayList<>(List.of(VARCHAR)), relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationS = relationNamePrefix + "S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(List.of(Arrays.asList(new StringValue("red"), new StringValue("3"), new StringValue("2"))));
            for (int i = 0; i < numberOfTuplesRemovedFromHashTableS; ++i) {
                relationValS.add(Arrays.asList(new StringValue("red"), new StringValue("1"), new StringValue("2")));
            }
            jdbcClient.ingestRelation(schemaName, relationS, new ArrayList<>(Arrays.asList("x", "y", "z")), new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)), relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationB = relationNamePrefix + "B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = List.of(Collections.singletonList(new StringValue("2")));
            jdbcClient.ingestRelation(schemaName, relationB, new ArrayList<>(List.of("z")), new ArrayList<>(List.of(VARCHAR)), relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        String relationR = relationNamePrefix + "R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(List.of(Arrays.asList(new StringValue("3"), new StringValue("2"))));
            jdbcClient.ingestRelation(schemaName, relationR, new ArrayList<>(Arrays.asList("y", "z")), new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)), relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeB), asEdge(nodeS, nodeR))), nodeT);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameB, schemaTableNameR))), Optional.empty());
    }

    /**
     * This is the same purpose as testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseFive but we show
     * good tuples in the internal node in join tree, e.g., S, can also be marked as no-good; not just good tuples in table scan
     * operator as shown in caseFive.
     */
    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseSix()
    {
        JdbcClient jdbcClient = base.getDatabase().getJdbcClient();
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "casesix_";
        String relationT = relationNamePrefix + "T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(Collections.singletonList(new StringValue("red")), Collections.singletonList(new StringValue("red"))));
            jdbcClient.ingestRelation(schemaName, relationT, new ArrayList<>(List.of("x")), new ArrayList<>(List.of(VARCHAR)), relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationS = relationNamePrefix + "S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(List.of(Arrays.asList(new StringValue("red"), new StringValue("3"), new StringValue("2"))));
            jdbcClient.ingestRelation(schemaName, relationS, new ArrayList<>(Arrays.asList("x", "y", "z")), new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR, VARCHAR)), relationValS);
        }
        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, domainS);

        String relationB = relationNamePrefix + "B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(Arrays.asList(new StringValue("3"), new StringValue("2")), Arrays.asList(new StringValue("3"), new StringValue("4"))));
            jdbcClient.ingestRelation(schemaName, relationB, new ArrayList<>(Arrays.asList("y", "w")), new ArrayList<>(Arrays.asList(VARCHAR, VARCHAR)), relationValB);
        }
        MultiwayJoinDomain domainB = new MultiwayJoinDomain();
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, domainB);

        String relationR = relationNamePrefix + "R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(Arrays.asList(new StringValue("2"))));
            jdbcClient.ingestRelation(schemaName, relationR, new ArrayList<>(Arrays.asList("w")), new ArrayList<>(Arrays.asList(VARCHAR)), relationValR);
        }
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, domainR);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeT, nodeS), asEdge(nodeS, nodeB), asEdge(nodeB, nodeR))), nodeT);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameS, schemaTableNameB, schemaTableNameR))), Optional.empty());
    }

    public Pair<Plan, List<Operator>> testTupleBaseTreeTrackerOneBetaHashTableOperatorComplexCaseSeven()
    {
        String schemaName = base.getDatabase().getSchemaName();
        String relationNamePrefix = "caseseven_";
        String relationC = relationNamePrefix + "_C";
        SchemaTableName schemaTableNameC = new SchemaTableName(schemaName, relationC);
        List<String> attributesC = List.of("y");
        if (jdbcClient.getTableHandle(schemaTableNameC) == null) {
            List<List<RelationalValue>> relationValC = new ArrayList<>(Arrays.asList(Arrays.asList(new IntegerValue(1)), Arrays.asList(new IntegerValue(1)), Arrays.asList(new IntegerValue(1)), Arrays.asList(new IntegerValue(1)), Arrays.asList(new IntegerValue(3)), Arrays.asList(new IntegerValue(6)), Arrays.asList(new IntegerValue(5))));
            jdbcClient.ingestRelation(schemaName, relationC, attributesC, new ArrayList<>(Arrays.asList(INTEGER)), relationValC);
        }
        MultiwayJoinNode nodeC = new MultiwayJoinNode(schemaTableNameC, attributesC, new MultiwayJoinDomain());

        String relationR = relationNamePrefix + "_R";
        SchemaTableName schemaTableNameR = new SchemaTableName(schemaName, relationR);
        List<String> attributesR = List.of("x", "y");
        if (jdbcClient.getTableHandle(schemaTableNameR) == null) {
            List<List<RelationalValue>> relationValR = new ArrayList<>(Arrays.asList(Arrays.asList(new IntegerValue(2), new IntegerValue(3)), Arrays.asList(new IntegerValue(2), new IntegerValue(3)), Arrays.asList(new IntegerValue(2), new IntegerValue(3)), Arrays.asList(new IntegerValue(2), new IntegerValue(4)), Arrays.asList(new IntegerValue(2), new IntegerValue(6)), Arrays.asList(new IntegerValue(2), new IntegerValue(5))));
            jdbcClient.ingestRelation(schemaName, relationR, attributesR, new ArrayList<>(Arrays.asList(INTEGER, INTEGER)), relationValR);
        }
        MultiwayJoinNode nodeR = new MultiwayJoinNode(schemaTableNameR, attributesR, new MultiwayJoinDomain());

        String relationB = relationNamePrefix + "_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        List<String> attributesB = List.of("x", "y");
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(Arrays.asList(new IntegerValue(2), new IntegerValue(4)), Arrays.asList(new IntegerValue(2), new IntegerValue(6)), Arrays.asList(new IntegerValue(2), new IntegerValue(5))));
            jdbcClient.ingestRelation(schemaName, relationB, attributesB, new ArrayList<>(Arrays.asList(INTEGER, INTEGER)), relationValB);
        }
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, attributesB, new MultiwayJoinDomain());

        String relationG = relationNamePrefix + "_G";
        SchemaTableName schemaTableNameG = new SchemaTableName(schemaName, relationG);
        List<String> attributesG = List.of("y");
        if (jdbcClient.getTableHandle(schemaTableNameG) == null) {
            List<List<RelationalValue>> relationValG = new ArrayList<>(Arrays.asList(Arrays.asList(new IntegerValue(5)), Arrays.asList(new IntegerValue(5))));
            jdbcClient.ingestRelation(schemaName, relationG, attributesG, new ArrayList<>(Arrays.asList(INTEGER)), relationValG);
        }
        MultiwayJoinNode nodeG = new MultiwayJoinNode(schemaTableNameG, attributesG, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(asEdge(nodeC, nodeR), asEdge(nodeR, nodeB), asEdge(nodeB, nodeG))), nodeC);

        return base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.empty(), Optional.empty());
    }
}
