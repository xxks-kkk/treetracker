package org.zhu45.treetracker.benchmark.job.q19;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.JoinFragmentContext;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.job.JOBQueries;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.Arrays;
import java.util.List;

import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getAkaNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getCastInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getInfoTypeInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getMovieInfoInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getNameInt;
import static org.zhu45.treetracker.benchmark.job.IMDBDatabase.getTitleInt;
import static org.zhu45.treetracker.relational.planner.rule.JoinOrdering.getJoinOrderingFromNodes;

/**
 * Figure out the estimation error compared to TTJ true cost is due to the limitation of model itself
 * or due to the estimation from Postgres.
 */
public class Query19aEstimationTroubleShooting
        extends Query
{
    public Query19aEstimationTroubleShooting(JoinFragmentContext context)
    {
        super(context);
    }

    @Override
    protected Pair<Plan, List<Operator>> constructQuery()
    {
        MultiwayJoinNode castInfoNode = getCastInfoInt(JOBQueries.Q19a);
        MultiwayJoinNode akaNameNode = getAkaNameInt(JOBQueries.Q19a);
        MultiwayJoinNode nameNode = getNameInt(JOBQueries.Q19a);
        MultiwayJoinNode movieInfoNode = getMovieInfoInt(JOBQueries.Q19a);
        MultiwayJoinNode titleNode = getTitleInt(JOBQueries.Q19a, null);
        MultiwayJoinNode infoTypeNode = getInfoTypeInt(JOBQueries.Q19a, null);

        Pair<Plan, List<Operator>> pair = createPhysicalPlanFromJoinOrdering(getJoinOrderingFromNodes(List.of(nameNode, akaNameNode, castInfoNode, titleNode, movieInfoNode, infoTypeNode)));
        Plan plan = pair.getKey();

        verifyJoinOrdering(plan, Arrays.asList(nameNode.getSchemaTableName(),
                akaNameNode.getSchemaTableName(),
                castInfoNode.getSchemaTableName(),
                titleNode.getSchemaTableName(),
                movieInfoNode.getSchemaTableName(),
                infoTypeNode.getSchemaTableName()));
        return pair;
    }
}
