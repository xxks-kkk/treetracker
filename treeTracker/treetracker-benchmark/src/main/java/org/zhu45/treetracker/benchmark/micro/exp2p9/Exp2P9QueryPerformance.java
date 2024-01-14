package org.zhu45.treetracker.benchmark.micro.exp2p9;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.Query;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.execution.ExecutionNormal;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.TreeTrackerBFJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHashJoinOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTableScanOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator;
import org.zhu45.treetracker.relational.operator.TupleBasedTableScanOperator;

import java.util.List;

import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.TestConstants.TREETRACKER_DEBUG;
import static org.zhu45.treetracker.common.TestConstants.checkEnvVariableSet;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

public class Exp2P9QueryPerformance
{
    private Exp2P9QueryPerformance()
    {
    }

    public static void run(JoinOperator algorithm, Class<? extends Query> queryClazz)
    {
        if (checkEnvVariableSet(TREETRACKER_DEBUG)) {
            Configurator.setAllLevels(TreeTrackerBFJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTreeTrackerOneBetaHashTableOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHighPerfTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedHashJoinOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(TupleBasedTableScanOperator.class.getName(), Level.TRACE);
            Configurator.setAllLevels(ExecutionNormal.class.getName(), Level.TRACE);
        }
        System.out.println("JoinOperator: " + algorithm);
        System.out.println("queryClazz: " + queryClazz);
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(algorithm)
                .setQueryClazz(queryClazz)
                .setRules(List.of())
                .setJdbcClient(postgresJdbcClientSupplier.get())
                .build();
        JoinFragmentType query = queryProvider(context);
        try {
            query.eval();
        }
        finally {
            query.getOperators().forEach(operator -> {
                if (operator.getMultiwayJoinNode() != null) {
                    MultiwayJoinNode node = operator.getMultiwayJoinNode();
                    node.getDomain().close();
                }
                operator.close();
            });
        }
    }

    public static void main(String[] args)
    {
        run(JoinOperator.TTJHP_BF, Exp2P9Query40P.class);
    }
}
