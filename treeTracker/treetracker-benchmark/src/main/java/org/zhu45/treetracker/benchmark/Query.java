package org.zhu45.treetracker.benchmark;

import org.apache.commons.lang3.tuple.Pair;
import org.zhu45.treetracker.common.SchemaTableName;
import org.zhu45.treetracker.relational.OptType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.Operator;
import org.zhu45.treetracker.relational.planner.Plan;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public abstract class Query
        extends JoinFragment
{
    protected Boolean isDevMode;
    protected Map<OptType, List<Class<? extends Operator>>> operatorMap;
    protected long backjumpedRelationSize;
    protected int numberOfBackJumpedRelations;
    protected int numRelations;
    protected JoinOperator algorithm;
    private final String queryName;

    public Query(JoinFragmentContext context)
    {
        this.jdbcClient = requireNonNull(context.getJdbcClient(), "jdbcClient not set");
        this.isDevMode = context.isDevMode();
        this.rules = context.getRules();
        this.operatorMap = context.getOperatorMap();
        this.noGoodList = context.getNoGoodList();
        this.backjumpedRelationSize = context.getBackjumpedRelationSize();
        this.numberOfBackJumpedRelations = context.getNumberOfBackJumpedRelations();
        this.algorithm = context.getAlgorithm();
        this.queryName = context.getQueryName() == null ? this.getClass().getCanonicalName() : context.getQueryName();
        this.stopAfterFullReducer = context.getStopAfterFullReducer();
        this.disablePTOptimizationTrick = context.isDisablePTOptimizationTrick();
        initializeQuery();
    }

    private void initializeQuery()
    {
        if (operatorMap != null) {
            setOperatorMap(operatorMap);
        }
        Pair<Plan, List<Operator>> pair = constructQuery();
        setPhysicalPlan(pair.getKey());
        setNumRelations(pair.getKey());
        operators = pair.getValue();
    }

    private void setNumRelations(Plan plan)
    {
        numRelations = plan.getRoot().getOperator().getPlanBuildContext().getSchemaTableNameList().size();
    }

    public int getNumRelations()
    {
        return numRelations;
    }

    public String getQueryName()
    {
        return queryName;
    }

    @Override
    public JoinOperator getAlgorithm()
    {
        return algorithm;
    }

    protected abstract Pair<Plan, List<Operator>> constructQuery();

    public void verifyJoinOrdering(Plan plan, List<SchemaTableName> schemaTableNameList)
    {
        if (plan.getPlanStatistics().getOptimalJoinOrdering() != null) {
            checkState(caseVerifier.visitPlan(plan.getRoot(),
                    new LinkedList<>(plan.getPlanStatistics().getOptimalJoinOrdering().getSchemaTableNameList())));
        }
        else {
            LinkedList<SchemaTableName> expectedSchemaTableNames = new LinkedList<>(schemaTableNameList);
            checkState(caseVerifier.visitPlan(plan.getRoot(), expectedSchemaTableNames));
        }
    }
}
