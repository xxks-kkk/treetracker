package org.zhu45.treetracker.common;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.io.IOException;
import java.io.InputStream;

public class RedissonClientSupplier
{
    private RedissonClientSupplier()
    {
    }

    public static Supplier<RedissonClient> redissonClientSupplier = Suppliers.memoize(() -> {
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            InputStream is = classLoader.getResourceAsStream("redisson.yaml");
            Config config = Config.fromYAML(is);
            return Redisson.create(config);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    });
}
