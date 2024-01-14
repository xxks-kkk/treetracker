package org.zhu45.treetracker.benchmark.job.q32;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getLinkTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieLinkInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query322
        extends Query
{
    public Query322(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieLinkNode = getMovieLinkInt();
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q32, null);
        MultiwayJoinNode linkTypeNode = getLinkTypeInt(JOBQueries.Q32);
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q32);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieKeywordNode, movieLinkNode),
                asEdge(movieLinkNode, linkTypeNode),
                asEdge(movieKeywordNode, keywordNode),
                asEdge(movieKeywordNode, titleNode)), titleNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

//        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
//                Arrays.asList(movieKeywordNode.getSchemaTableName(),
//                        movieLinkNode.getSchemaTableName(),
//                        linkTypeNode.getSchemaTableName(),
//                        keywordNode.getSchemaTableName(),
//                        titleNode.getSchemaTableName()));
//        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
