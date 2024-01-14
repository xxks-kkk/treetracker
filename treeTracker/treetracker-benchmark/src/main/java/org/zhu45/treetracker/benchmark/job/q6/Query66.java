package org.zhu45.treetracker.benchmark.job.q6;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query66
        extends Query
{
    public Query66(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q6);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q6, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q6);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q6);

        // Inspired from Q262, Q25
        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(castInfoNode, nameNode),
                asEdge(castInfoNode, titleNode),
                asEdge(castInfoNode, movieKeywordNode),
                asEdge(movieKeywordNode, keywordNode)), castInfoNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(castInfoNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        titleNode.getSchemaTableName(),
                        movieKeywordNode.getSchemaTableName(),
                        keywordNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
