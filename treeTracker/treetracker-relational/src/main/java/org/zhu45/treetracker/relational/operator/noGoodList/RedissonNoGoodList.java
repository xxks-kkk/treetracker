package org.zhu45.treetracker.relational.operator.noGoodList;

import org.redisson.api.RSet;
import org.redisson.api.RedissonClient;
import org.zhu45.treetracker.common.Value;

import java.util.UUID;

import static org.zhu45.treetracker.common.RedissonClientSupplier.redissonClientSupplier;

public class RedissonNoGoodList
        implements NoGoodList<Value>, AutoCloseable
{
    private final RSet<Value> noGoodList;
    private final RedissonClient redissonClient;

    private RedissonNoGoodList()
    {
        redissonClient = redissonClientSupplier.get();
        String redisNoGoodListSchema = "noGoodList";
        noGoodList = redissonClient.getSet(redisNoGoodListSchema + UUID.randomUUID());
    }

    @Override
    public int size()
    {
        return noGoodList.size();
    }

    @Override
    public boolean contains(Value value)
    {
        return noGoodList.contains(value);
    }

    @Override
    public boolean add(Value value)
    {
        return noGoodList.add(value);
    }

    public static NoGoodList<Value> create()
    {
        return new RedissonNoGoodList();
    }

    @Override
    public void close()
            throws Exception
    {
        redissonClient.shutdown();
    }
}
