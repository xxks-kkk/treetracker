package org.zhu45.treetracker.benchmark.job.patterns;

import org.apache.commons.lang3.tuple.Pair;
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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCharNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.JOBQueries.NOPREDICATE;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class CastInfoPattern
        extends Query
{
    public CastInfoPattern(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(NOPREDICATE);
        MultiwayJoinNode akaNameNode = getAkaNameInt(NOPREDICATE);
        MultiwayJoinNode nameNode = getNameInt(NOPREDICATE);
        MultiwayJoinNode charNameNode = getCharNameInt(NOPREDICATE);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(castInfoNode, akaNameNode),
                asEdge(castInfoNode, nameNode),
                asEdge(castInfoNode, charNameNode)), castInfoNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(castInfoNode.getSchemaTableName(),
                        akaNameNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        charNameNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
