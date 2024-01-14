package org.zhu45.treetracker.benchmark;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinOrderedGraph;
import org.zhu45.treetracker.common.TreeTrackerException;
import org.zhu45.treetracker.jdbc.JdbcClient;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.statistics.CostModel;
import org.zhu45.treetracker.relational.statistics.CostModelContext;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static org.zhu45.treetracker.relational.operator.JoinOperator.Yannakakis;
import static org.zhu45.treetracker.relational.statistics.CostModelContext.builder;

public class CostModelFactory
{
    private JoinFragmentType query;
    private MultiwayJoinOrderedGraph joinTree;
    private Class<? extends CostModel> costModelClazz;
    private JdbcClient jdbcClient;

    public CostModelFactory(QueryEnum queryEnum,
            Class<? extends CostModel> costModelClazz,
            JdbcClient jdbcClient)
    {
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(Yannakakis)
                .setQueryClazz(queryEnum.getQueryClazz())
                .setStopAfterFullReducer(true)
                .setJdbcClient(jdbcClient)
                .build();
        this.query = queryProvider(context);
        this.joinTree = query.getPlan().getRoot().getOperator().getPlanBuildContext().getOrderedGraph();
        this.jdbcClient = query.getPlan().getRoot().getOperator().getPlanBuildContext().getJdbcClient();
        this.costModelClazz = costModelClazz;
    }

    public <T extends CostModel> CostModel get()
    {
        try {
            Constructor<? extends CostModel> constructor = costModelClazz.getConstructor(CostModelContext.class);
            CostModelContext context = builder()
                    .setOrderedGraph(joinTree)
                    .setJdbcClient(jdbcClient)
                    .setJoinFragmentType(query)
                    .setPlanNodeIdAllocator(query.getRootOperator().getPlanBuildContext().getPlanNodeIdAllocator())
                    .build();
            return constructor.newInstance(context);
        }
        catch (NoSuchMethodException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR,
                    String.format("There is no constructor found for %s", costModelClazz.getName()));
        }
        catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            throw new TreeTrackerException(FUNCTION_IMPLEMENTATION_ERROR, e);
        }
    }

    public void cleanUp()
    {
        query.cleanUp();
    }
}
