package org.zhu45.treetracker.relational.operator.testCases;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinDomain;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treektracker.multiwayJoin.testing.TestingMultiwayJoinDatabase;
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

public class TestTupleBasedTreeTrackerTwoOperatorCases
        implements TestCases
{
    private static CharType charType = createCharType(20);
    private TestingPhysicalPlanBase base;
    private String schemaName;
    private JdbcClient jdbcClient;

    public TestTupleBasedTreeTrackerTwoOperatorCases(TestingPhysicalPlanBase base)
    {
        this.base = base;
        this.schemaName = base.getDatabase().getSchemaName();
        this.jdbcClient = base.getDatabase().getJdbcClient();
    }

    /**
     * This test case shows why we need to clear g_2 and not remove the last element when C_2 is true. In other words,
     * the idea of using C_2 doesn't seem to work (commit: f415e23a3ad6035a3be64905bce17b6e20ab017d).
     */
    public Pair<Plan, List<Operator>> testTupleBasedTreeTrackerTwoOperatorComplexCaseOne()
    {
        String relationT = "caseone_T";
        SchemaTableName schemaTableNameT = new SchemaTableName(schemaName, relationT);
        if (jdbcClient.getTableHandle(schemaTableNameT) == null) {
            List<List<RelationalValue>> relationValT = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue(charType, "red")),
                    Collections.singletonList(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationT,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValT);
        }
        MultiwayJoinDomain domainT = new MultiwayJoinDomain();
        MultiwayJoinNode nodeT = new MultiwayJoinNode(schemaTableNameT, domainT);

        String relationB = "caseone_B";
        SchemaTableName schemaTableNameB = new SchemaTableName(schemaName, relationB);
        if (jdbcClient.getTableHandle(schemaTableNameB) == null) {
            List<List<RelationalValue>> relationValB = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue(charType, "red")),
                    Collections.singletonList(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationB,
                    new ArrayList<>(List.of("color")),
                    new ArrayList<>(List.of(charType)),
                    relationValB);
        }
        MultiwayJoinNode nodeB = new MultiwayJoinNode(schemaTableNameB, new MultiwayJoinDomain());

        String relationS = "caseone_S";
        SchemaTableName schemaTableNameS = new SchemaTableName(schemaName, relationS);
        if (jdbcClient.getTableHandle(schemaTableNameS) == null) {
            List<List<RelationalValue>> relationValS = new ArrayList<>(Arrays.asList(
                    Collections.singletonList(new StringValue(charType, "black")),
                    Collections.singletonList(new StringValue(charType, "red"))));
            jdbcClient.ingestRelation(
                    schemaName,
                    relationS,
                    new ArrayList<>(Arrays.asList("color")),
                    new ArrayList<>(Arrays.asList(charType)),
                    relationValS);
        }
        MultiwayJoinNode nodeS = new MultiwayJoinNode(schemaTableNameS, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeT, nodeB),
                asEdge(nodeB, nodeS))), nodeT);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                        new LinkedList<>(Arrays.asList(schemaTableNameT, schemaTableNameB, schemaTableNameS))),
                Optional.empty());
        Plan plan = pair.getKey();
        plan.swapAll();
        return pair;
    }

    /**
     * This case shows why we don't have getR2() call at the very end of if(r2=nil) block. In other words,
     * we only need one getR2() call. The plan looks like: (join (join (join S U) A) R).
     * <p>
     * seed_plan: 2212631671581782360L
     */
    public Pair<Plan, List<Operator>> testTupleBasedTreeTrackerTwoOperatorCaseOne()
    {
        String schemaName = TestingMultiwayJoinDatabase.schemaName;
        SchemaTableName s = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.S.getRelationName());
        SchemaTableName u = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.U.getRelationName());
        SchemaTableName a = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.A.getRelationName());
        SchemaTableName r = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.R.getRelationName());

        MultiwayJoinDomain domainS = new MultiwayJoinDomain();
        MultiwayJoinDomain domainU = new MultiwayJoinDomain();
        MultiwayJoinDomain domainA = new MultiwayJoinDomain();
        MultiwayJoinDomain domainR = new MultiwayJoinDomain();

        MultiwayJoinNode nodeS = new MultiwayJoinNode(s, domainS);
        MultiwayJoinNode nodeU = new MultiwayJoinNode(u, domainU);
        MultiwayJoinNode nodeA = new MultiwayJoinNode(a, domainA);
        MultiwayJoinNode nodeR = new MultiwayJoinNode(r, domainR);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeS, nodeU),
                asEdge(nodeU, nodeA),
                asEdge(nodeU, nodeR))), nodeS);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                        new LinkedList<>(Arrays.asList(s, u, a, r))),
                Optional.empty());

        // Check if the generated plan is what we want
        Plan plan = pair.getKey();
        plan.swapAll();
        return pair;
    }

    /**
     * This test case tests try-good-list(). In specific, it tests when the good-list contains join result,
     * when the downstream subtree generates the same join result, it should be ignored because the join result
     * is already used in the try-good-list().
     * <p>
     * In this example, we have (join1 (join2 T A) B). T \join2 A creates a join result (red)
     * that becomes part of the good-list in join1 when B is looking at its first (red) tuple. Now, B moves to the
     * second (red) tuple and try-good-list() used (red) from its good-list to create the second join result (red). Then,
     * the loop in binary operator continues and r2 is assigned value, which is T \join2 A result. Now, the implementation
     * should ignore this join result because T \join2 A has only one join tuple, which is already used in try-good-list().
     * Inproper implementation will lead to duplicate join tuples generated.
     * <p>
     * seed_plan: -4061262230290821524L
     */
    public Pair<Plan, List<Operator>> testTupleBasedTreeTrackerTwoOperatorCaseTwo()
    {
        String schemaName = TestingMultiwayJoinDatabase.schemaName;
        SchemaTableName b = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.B.getRelationName());
        MultiwayJoinNode nodeB = new MultiwayJoinNode(b, new MultiwayJoinDomain());

        SchemaTableName a = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.A.getRelationName());
        MultiwayJoinNode nodeA = new MultiwayJoinNode(a, new MultiwayJoinDomain());

        SchemaTableName t = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.T.getRelationName());
        MultiwayJoinNode nodeT = new MultiwayJoinNode(t, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeT, nodeA),
                asEdge(nodeA, nodeB))), nodeT);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                        new LinkedList<>(Arrays.asList(t, a, b))),
                Optional.empty());
        Plan plan = pair.getKey();
        plan.swapAll();
        return pair;
    }

    /**
     * This test case shows the impact of RowID refactor: the same join tuple has the fixed RowId (i.e., it doesn't get different
     * id each time it is computed from the same rows identified by their ids) and the impact of the execution ordering
     * between try-good-list (L17) and reset() (L20 & L21).
     * <p>
     * The plan looks like (join1 (join2 (join3 S R) A) B). Suppose the physical plan is left-deep join tree with
     * r2 binds to left (inner loop of Nested-Loop Join (NLJ)) and r1 binds to right (i.e., outer loop of NLJ). Note
     * r2 and r1 bindings are not efficient ones: the execution can construct intermediate relation more than once.
     * In addition, we use A > B to indicate statement A executes before statement B.
     * <p>
     * The conclusion is:
     * <p>
     * 1. Without RowID refactor, try-good-list > reset() will ensure the query evaluation correctness. In other words,
     * if try-good-list > reset(), either UUID-based RowID or List<UUID>-base RowID can ensure correctness.
     * 2. If try-good-list < reset(), we need RowID refactor and the property it brings to ensure correctness.
     * <p>
     * In this example, it is expected to have two (red) as the correct join result but violation of the above conclusion
     * will ead to three (red) as the join result.
     * <p>
     * To see this, let's assume that there is no RowId refactor and try-good-list < reset().
     * Then three (red) are computed as following:
     * <p>
     * 1. First (red) is computed when B is set to its first (red) tuple and subtree rooted with join2 is computed and the
     * result is joined with B's (red).
     * 2. getNext() is called to get next joined tuple. Then subtree rooted with join2 is traversed and realized that there
     * is no more (red) that can be joined with B's first (red). Then B sets to the second (red) (L15 of the algorithm)
     * and iterators in the subtree rooted with join2 is reset() and then try-good-list is called and
     * then the second joined tuple (red) is found.
     * 3. getNext() is called again and getR2() is invoked from join1 (L9 in algorithm). After some loops due to
     * unable join between R and S or the join of R and S is not able to join A, R get (red) in L15 and then due
     * to try-good-list, S gets (red) (because (red) is in good-list of join3). Then join of R and S is returned,
     * which is set to r2 in join2 as the return of calling getR2() (ISSUE! EXPLAIN NEXT PRARGRAPH). Since r1 in join2 is set to (red) in reset when
     * finding the second joined tuple (red), r1 and r2 are joined at the end of getNext() of join2 and the result
     * is returned. Similarly for join1, the result returned from join2 is set to r2, which as the return of getR2()
     * is join1. Then, for the same reason as join2, r1 and r2 join together to form the third joined tuple of the final
     * result set.
     * <p>
     * ISSUE! As one can see, the redundant joined tuple is the third one: because join R and S get a new RowId, the checking
     * within getR2() of join2 fails: it considers the joined tuple as a new one. Same happens for getR2() in join1.
     * <p>
     * seed_plan: 521941785023752515L
     */
    public Pair<Plan, List<Operator>> testTupleBasedTreeTrackerTwoOperatorCaseThree()
    {
        String schemaName = TestingMultiwayJoinDatabase.schemaName;
        SchemaTableName b = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.B.getRelationName());
        MultiwayJoinNode nodeB = new MultiwayJoinNode(b, new MultiwayJoinDomain());

        SchemaTableName a = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.A.getRelationName());
        MultiwayJoinNode nodeA = new MultiwayJoinNode(a, new MultiwayJoinDomain());

        SchemaTableName s = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.S.getRelationName());
        MultiwayJoinNode nodeS = new MultiwayJoinNode(s, new MultiwayJoinDomain());

        SchemaTableName r = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.R.getRelationName());
        MultiwayJoinNode nodeR = new MultiwayJoinNode(r, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeS, nodeR),
                asEdge(nodeR, nodeA),
                asEdge(nodeA, nodeB))), nodeS);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                        new LinkedList<>(Arrays.asList(s, r, a, b))),
                Optional.empty());
        Plan plan = pair.getKey();
        plan.swapAll();
        return pair;
    }

    /**
     * This case shows why we need to clear goodList in reset(). Details see 5.2.1.1 Algorithm Remarks point 2.
     * <p>
     * seed_plan: 1628791995761025382L
     */
    public Pair<Plan, List<Operator>> testTupleBasedTreeTrackerTwoOperatorCaseFour()
    {
        String schemaName = TestingMultiwayJoinDatabase.schemaName;
        SchemaTableName s = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.S.getRelationName());
        MultiwayJoinNode nodeS = new MultiwayJoinNode(s, new MultiwayJoinDomain());

        SchemaTableName t = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.T.getRelationName());
        MultiwayJoinNode nodeT = new MultiwayJoinNode(t, new MultiwayJoinDomain());

        SchemaTableName b = new SchemaTableName(schemaName, TestingMultiwayJoinDatabase.RelationData.B.getRelationName());
        MultiwayJoinNode nodeB = new MultiwayJoinNode(b, new MultiwayJoinDomain());

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(new ArrayList<>(Arrays.asList(
                asEdge(nodeT, nodeB),
                asEdge(nodeB, nodeS))), nodeT);

        Pair<Plan, List<Operator>> pair = base.createFixedPhysicalPlanFromQueryGraph(orderedGraph, Optional.of(
                        new LinkedList<>(Arrays.asList(t, b, s))),
                Optional.empty());
        Plan plan = pair.getKey();
        plan.swapAll();
        return pair;
    }
}
