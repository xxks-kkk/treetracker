package org.zhu45.treetracker.benchmark.job.q18;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.benchmark.job.TableInstanceId;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.DirectedEdge.asDirectedEdge;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

public class Query18aOptJoinTreeOptOrdering
        extends Query
{
    public Query18aOptJoinTreeOptOrdering(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q18a, TableInstanceId.TWO);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q18a);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q18a, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q18a, TableInstanceId.TWO);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q18a);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q18a);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q18a, null);

        List<MultiwayJoinNode> traversalList = List.of(movieInfoNode, infoTypeNode1, movieInfoIdxNode2, infoTypeNode2, castInfoNode, nameNode, titleNode);

        MultiwayJoinOrderedGraph orderedGraph = new MultiwayJoinOrderedGraph(movieInfoNode, Arrays.asList(
                asDirectedEdge(movieInfoNode, infoTypeNode1),
                asDirectedEdge(movieInfoNode, movieInfoIdxNode2),
                asDirectedEdge(movieInfoIdxNode2, infoTypeNode2),
                asDirectedEdge(movieInfoNode, castInfoNode),
                asDirectedEdge(castInfoNode, nameNode),
                asDirectedEdge(movieInfoNode, titleNode)), traversalList);

        Plan plan = createPhysicalPlanFromJoinOrdering(
                getJoinOrderingFromNodes(traversalList), orderedGraph);

        verifyJoinOrdering(plan, Arrays.asList(movieInfoNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                movieInfoIdxNode2.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                titleNode.getSchemaTableName()));

        return Pair.of(plan, plan.getOperatorList());
    }
}
