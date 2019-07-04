package com.mt.fbi.livy.client;

import com.mt.fbi.livy.util.Helper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;

/**
 * Created by yihaibo on 2019-04-22.
 */
@Slf4j
public class PoolInteractiveClientFactory implements PooledObjectFactory<InteractiveSessionClient> {
    private InteractiveSessionClient.Builder builder;

    public PoolInteractiveClientFactory(InteractiveSessionClient.Builder builder) {
        this.builder = builder;
    }

    @Override
    public PooledObject<InteractiveSessionClient> makeObject() throws Exception {
        InteractiveSessionClient client = builder.build();
        client.awaitOpen();
        return new DefaultPooledObject<>(client);
    }

    @Override
    public void destroyObject(PooledObject<InteractiveSessionClient> p) throws Exception {
        InteractiveSessionClient client = p.getObject();
        client.close();
    }

    @Override
    public boolean validateObject(PooledObject<InteractiveSessionClient> p) {
        InteractiveSessionClient client = p.getObject();
        client.ping();
        try{
            return !client.getSessionInfo().isFinished();
        }catch (Exception e) {
            Helper.sleep(500);
            try {
                return !client.getSessionInfo().isFinished();
            }catch (Exception e2) {
                log.error("validateObject, e:",  e2);
                return false;
            }
        }
    }

    @Override
    public void activateObject(PooledObject<InteractiveSessionClient> p) throws Exception {

    }

    @Override
    public void passivateObject(PooledObject<InteractiveSessionClient> p) throws Exception {

    }
}
