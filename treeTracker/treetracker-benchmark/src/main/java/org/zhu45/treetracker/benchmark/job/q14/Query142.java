package org.zhu45.treetracker.benchmark.job.q14;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getKindTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieKeywordInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query142
        extends Query
{
    public Query142(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q14);
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q14, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q14, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q14, null);
        MultiwayJoinNode kindTypeNode = getKindTypeInt(JOBQueries.Q14, null);
        MultiwayJoinNode movieKeywordNode = getMovieKeywordInt();
        MultiwayJoinNode keywordNode = getKeywordInt(JOBQueries.Q14);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieInfoNode, movieInfoIdxNode),
                asEdge(movieInfoIdxNode, titleNode),
                asEdge(movieInfoIdxNode, infoTypeNode),
                asEdge(titleNode, movieKeywordNode),
                asEdge(titleNode, kindTypeNode),
                asEdge(movieKeywordNode, keywordNode)), infoTypeNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

//        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
//                Arrays.asList(movieInfoNode.getSchemaTableName(),
//                        movieInfoIdxNode.getSchemaTableName(),
//                        titleNode.getSchemaTableName(),
//                        movieKeywordNode.getSchemaTableName(),
//                        keywordNode.getSchemaTableName(),
//                        kindTypeNode.getSchemaTableName(),
//                        infoTypeNode.getSchemaTableName()));
//        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
