package com.mt.fbi.livy.client;

import com.mt.fbi.livy.util.JsonUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yihaibo on 2019-04-19.
 */
public class InteractiveSessionClientTest {

    private Map<String, Object> livyConf() {
        Map<String, Object> livyConf = new HashMap<>();
        livyConf.put("queue", "queue");
        livyConf.put("proxyUser", "user");
        Map<String, String> conf = new HashMap<>();
        conf.put("livy.rsc.proxy-password", "passwd");
        livyConf.put("conf", conf);
        return livyConf;
    }

    private InteractiveSessionClient buildClient(int currentSessionId) {
        InteractiveSessionClient.Builder builder = InteractiveSessionClient.builder()
                .livyURL("http://localhost:8010")
                .restartDeadSession(true)
                .name("test")
                .kind(SessionKind.SQL)
                .livyConf(livyConf());
        if(currentSessionId > 0) {
            builder.currentSessionId(currentSessionId);
        }
        return builder.build();
    }

    @Test
    public void testCorrect() throws Exception {
        InteractiveSessionClient client = buildClient(-1);
        client.awaitOpen();
        InteractiveSessionClient.StatementInfo statementInfo = client.awaitExecuteStatement("show databases");
        System.out.println(JsonUtil.encode(statementInfo));
    }

    @Test(expected = LivyException.class)
    public void testSyntaxError() throws Exception {
        InteractiveSessionClient client = buildClient(-1);
        client.awaitOpen();
        InteractiveSessionClient.StatementInfo statementInfo = client.awaitExecuteStatement("show databases;show tables;");
        System.out.println(JsonUtil.encode(statementInfo));
    }

    @Test
    public void testSessionIdExist() throws Exception {
        InteractiveSessionClient client = buildClient(664);
        client.awaitOpen();
        System.out.println(JsonUtil.encode(client.getCurrentSessionInfo()));
        InteractiveSessionClient.StatementInfo statementInfo = client.awaitExecuteStatement("show databases");
        System.out.println(JsonUtil.encode(statementInfo));
    }

    @Test
    public void testDelete() throws Exception {
        InteractiveSessionClient client = buildClient(1440);
        client.awaitOpen();
        client.close();
    }
}
