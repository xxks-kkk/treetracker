package org.zhu45.treetracker.benchmark.job.q5;

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
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCompanyTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieCompaniesInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.common.Edge.asEdge;

public class Query54
        extends Query
{
    public Query54(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode movieCompaniesNode = getMovieCompaniesInt(JOBQueries.Q5, null);
        MultiwayJoinNode companyTypeNode = getCompanyTypeInt(JOBQueries.Q5);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q5);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q5, null);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q5, null);

        MultiwayJoinOrderedGraph orderedGraph = getMultiwayJoinOrderedGraph(Arrays.asList(
                asEdge(movieCompaniesNode, companyTypeNode),
                asEdge(movieCompaniesNode, movieInfoNode),
                asEdge(movieInfoNode, infoTypeNode),
                asEdge(movieCompaniesNode, titleNode)), titleNode);

        Pair<Plan, List<Operator>> pair = createFixedPhysicalPlanFromQueryGraph(orderedGraph);
        Plan plan = pair.getKey();

//        LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(
//                Arrays.asList(movieCompaniesNode.getSchemaTableName(),
//                        companyTypeNode.getSchemaTableName(),
//                        movieInfoNode.getSchemaTableName(),
//                        infoTypeNode.getSchemaTableName(),
//                        titleNode.getSchemaTableName()));
//        assertTrue(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        return pair;
    }
}
