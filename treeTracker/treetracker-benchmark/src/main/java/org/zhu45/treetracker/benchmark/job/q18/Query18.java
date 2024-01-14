package org.zhu45.treetracker.benchmark.job.q18;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoIdxInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query18
        extends Query
{
    public Query18(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieInfoIdxNode = getMovieInfoIdxInt(JOBQueries.Q18, null);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q18);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q18, null);
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q18);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q18);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q18, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieInfoIdxNode, movieInfoNode),
                asEdge(movieInfoNode, infoTypeNode),
                asEdge(movieInfoNode, castInfoNode),
                asEdge(castInfoNode, nameNode),
                asEdge(castInfoNode, titleNode)), movieInfoIdxNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
                Arrays.asList(movieInfoIdxNode.getSchemaTableName(),
                        movieInfoNode.getSchemaTableName(),
                        infoTypeNode.getSchemaTableName(),
                        castInfoNode.getSchemaTableName(),
                        nameNode.getSchemaTableName(),
                        titleNode.getSchemaTableName()));
        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
