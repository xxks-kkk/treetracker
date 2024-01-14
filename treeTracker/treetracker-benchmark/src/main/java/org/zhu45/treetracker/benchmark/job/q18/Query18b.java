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

import static org.zhu45.treektracker.multiwayJoin.MultiwayJoinPreorderTraversalStrategy.getMultiwayJoinOrderedGraph;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query18b
        extends Query
{
    public Query18b(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieInfoIdxNode2 = getMovieInfoIdxInt(JOBQueries.Q18b, TableInstanceId.TWO);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q18b);
        MultiwayJoinNode infoTypeNode1 = getInfoTypeInt(JOBQueries.Q18b, TableInstanceId.ONE);
        MultiwayJoinNode infoTypeNode2 = getInfoTypeInt(JOBQueries.Q18b, TableInstanceId.TWO);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q18b);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q18b);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q18b, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieInfoIdxNode2, movieInfoNode),
                asEdge(movieInfoNode, infoTypeNode1),
                asEdge(infoTypeNode1, infoTypeNode2),
                asEdge(movieInfoNode, castInfoNode),
                asEdge(castInfoNode, nameNode),
                asEdge(castInfoNode, titleNode)), movieInfoIdxNode2);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(movieInfoIdxNode2.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                infoTypeNode1.getSchemaTableName(),
                infoTypeNode2.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                nameNode.getSchemaTableName(),
                titleNode.getSchemaTableName()));
        return pair;
    }
}
