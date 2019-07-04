package com.mt.fbi.livy.client;

import com.mt.fbi.livy.util.JsonUtil;
import com.mt.fbi.livy.util.OkHttpRestClient;
import com.mt.fbi.livy.util.RestClient;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by yihaibo on 2019-04-18.
 */
public class UtilTest {
    @Test
    public void testEnum() throws IOException {
        System.out.println("hello");

        SessionState sessionState = SessionState.BUSY;

        System.out.println(sessionState.getState());

        System.out.println(JsonUtil.encode(sessionState));
    }


    @Test
    public void testHttp() throws Exception {
        RestClient restClient = new OkHttpRestClient();
        System.out.println(restClient.get("http://www.baidu.com"));
    }
}
