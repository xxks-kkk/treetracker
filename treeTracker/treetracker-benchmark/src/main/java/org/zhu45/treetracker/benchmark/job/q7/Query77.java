package org.zhu45.treetracker.benchmark.job.q7;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getLinkTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieLinkInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getPersonInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query77
        extends Query
{
    public Query77(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q7);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q7, null);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q7);
        MultiwayJoinNode personInfoNode = getPersonInfoInt(JOBQueries.Q7);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q7);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q7, null);
        MultiwayJoinNode movieLinkNode = getMovieLinkInt();
        MultiwayJoinNode linkTypeNode = getLinkTypeInt(JOBQueries.Q7);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(castInfoNode, akaNameNode),
                asEdge(akaNameNode, personInfoNode),
                asEdge(personInfoNode, infoTypeNode),
                asEdge(akaNameNode, nameNode),
                asEdge(castInfoNode, movieLinkNode),
                asEdge(movieLinkNode, linkTypeNode),
                asEdge(castInfoNode, titleNode)), linkTypeNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

//        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
//                Arrays.asList(castInfoNode.getSchemaTableName(),
//                        akaNameNode.getSchemaTableName(),
//                        personInfoNode.getSchemaTableName(),
//                        infoTypeNode.getSchemaTableName(),
//                        nameNode.getSchemaTableName(),
//                        movieLinkNode.getSchemaTableName(),
//                        linkTypeNode.getSchemaTableName(),
//                        titleNode.getSchemaTableName()));
//        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
