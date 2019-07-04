# spark livy pool client

[Livy](https://github.com/cloudera/livy) is an open source REST interface for interacting with Apache Spark from anywhere.

This project use apache commons-pool2 provide a livy pool client.

## Usage

```
public class PoolInteractiveClientTest {

    private Map<String, Object> livyConf() {
        Map<String, Object> livyConf = new HashMap<>();
        livyConf.put("queue", "queue");
        livyConf.put("proxyUser", "user");
        Map<String, String> conf = new HashMap<>();
        conf.put("livy.rsc.proxy-password", "passwd");
        livyConf.put("conf", conf);
        return livyConf;
    }

    private InteractiveSessionClient.Builder builder() {
        InteractiveSessionClient.Builder builder = InteractiveSessionClient.builder()
                .livyURL("http://localhost:8010")
                .restartDeadSession(true)
                .name("test")
                .kind(SessionKind.SQL)
                .livyConf(livyConf());
        return builder;
    }

    @Test
    public void testCorrect2() throws Exception {
        PoolClient poolClient = new PoolInteractiveClient(builder());
       
        startThread("select 'thread1'", poolClient);
        startThread("select 'thread2'", poolClient);

        Thread.sleep(1000*60*2);
        startThread("select 'thread3'", poolClient);
        startThread("select 'thread4'", poolClient);
        Thread.sleep(1000*60*50);
    }

    private void startThread(String code, PoolClient poolClient) {
        new Thread(() -> {
            try {
                InteractiveSessionClient client = poolClient.getClient();
                System.out.println(code);
                InteractiveSessionClient.StatementInfo statementInfo = client.awaitExecuteStatement(code);
                System.out.println(client.getSessionInfo().getId() + JsonUtil.encode(statementInfo.getOutput()));
                poolClient.returnClient(client);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }).start();
    }
}


```

## todo

- Support Batch Session
