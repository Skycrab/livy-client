package com.mt.fbi.livy.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Created by yihaibo on 2019-04-22.
 */
@Slf4j
public class PoolInteractiveClient implements PoolClient {

    private GenericObjectPool<InteractiveSessionClient> objectPool;

    public PoolInteractiveClient(InteractiveSessionClient.Builder builder, GenericObjectPoolConfig config, AbandonedConfig abandonedConfig) {
        PoolInteractiveClientFactory factory = new PoolInteractiveClientFactory(builder);
        objectPool = new GenericObjectPool<>(factory, config, abandonedConfig);
    }

    public PoolInteractiveClient(InteractiveSessionClient.Builder builder, int maxTotal, int maxIdle, int minIdle) {
        PoolInteractiveClientFactory factory = new PoolInteractiveClientFactory(builder);
        GenericObjectPoolConfig config = new GenericObjectPoolConfig();
        config.setMaxTotal(maxTotal);
        config.setMaxIdle(maxIdle);
        config.setMinIdle(minIdle);
        config.setTestOnBorrow(true);
        config.setTestWhileIdle(true);
        // 连接保持空闲而不被驱逐的最长时间
        config.setMinEvictableIdleTimeMillis(1000L * 60L * 60L);
        // 检查连接池中空闲的连接间隔
        config.setTimeBetweenEvictionRunsMillis(1000L*60*1);

        AbandonedConfig abandonedConfig = new AbandonedConfig();
        abandonedConfig.setRemoveAbandonedOnBorrow(true);
        abandonedConfig.setRemoveAbandonedOnMaintenance(true);
        objectPool = new GenericObjectPool<>(factory, config, abandonedConfig);
    }

    public PoolInteractiveClient(InteractiveSessionClient.Builder builder) {
        this(builder, 2, 2, 2);
    }

    @Override
    public InteractiveSessionClient getClient() throws Exception{
        return objectPool.borrowObject();
    }

    @Override
    public void returnClient(InteractiveSessionClient client) {
        objectPool.returnObject(client);
    }
}
