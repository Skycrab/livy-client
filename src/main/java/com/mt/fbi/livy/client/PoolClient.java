package com.mt.fbi.livy.client;

/**
 * Created by yihaibo on 2019-04-22.
 */
public interface PoolClient {
    InteractiveSessionClient getClient() throws Exception;
    void returnClient(InteractiveSessionClient client);
}
