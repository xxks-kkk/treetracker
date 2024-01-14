package org.zhu45.treetracker.benchmark.backjump;

import org.zhu45.treektracker.multiwayJoin.MultiwayJoinNode;
import org.zhu45.treetracker.benchmark.QueryProviderContext;
import org.zhu45.treetracker.relational.JoinFragmentType;
import org.zhu45.treetracker.relational.operator.JoinOperator;
import org.zhu45.treetracker.relational.operator.StatisticsInformationPrinter;

import static org.zhu45.treetracker.benchmark.QueryProvider.queryProvider;
import static org.zhu45.treetracker.common.RedissonClientSupplier.redissonClientSupplier;
import static org.zhu45.treetracker.jdbc.JdbcSupplier.postgresJdbcClientSupplier;

public class BackJumpQueryPerformance
{
    private BackJumpQueryPerformance() {}

    public static void main(String[] args)
    {
        long timeNow = System.currentTimeMillis();
        StatisticsInformationPrinter printer = new StatisticsInformationPrinter();
        QueryProviderContext context = QueryProviderContext.builder()
                .setJoinOperator(JoinOperator.HASH_JOIN)
                .setQueryClazz(BackJumpQuery.class)
                .setBackJumpedRelationSize(2)
                .setNumberOfBackjumpedRelations(32)
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
            if (!redissonClientSupplier.get().isShutdown()) {
                redissonClientSupplier.get().shutdown();
            }
            System.out.println("runtime: " + (System.currentTimeMillis() - timeNow) + " ms");
            System.out.println(printer.print(query.getRootOperator()));
        }
    }
}
