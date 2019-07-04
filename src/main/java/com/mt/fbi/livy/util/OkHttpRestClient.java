package com.mt.fbi.livy.util;

import com.mt.fbi.livy.client.StatusException;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by yihaibo on 2019-04-19.
 */
@Slf4j
public class OkHttpRestClient implements RestClient {
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private OkHttpClient client;

    public OkHttpRestClient() {
        client = new OkHttpClient.Builder()
                .readTimeout(2000, TimeUnit.MILLISECONDS)
                .connectTimeout(2000, TimeUnit.MILLISECONDS)
                .build();
    }

    @Override
    public String get(String url) throws IOException {
        Request request = new Request.Builder().url(url).build();
        return callRequest(request);
    }

    @Override
    public String post(String url, Object data) throws IOException {
        RequestBody body = RequestBody.create(JSON, JsonUtil.encode(data));
        Request request = new Request.Builder().url(url).post(body).build();
        return callRequest(request);
    }

    @Override
    public void delete(String url) throws IOException {
        Request request = new Request.Builder().url(url).delete().build();
        callRequest(request);
    }

    private String callRequest(Request request) throws IOException {
        try (Response response = client.newCall(request).execute()) {
            if(response.code() != 200 && response.code() != 201 ) {
                log.error("request error, method:{} url:{} body:{} code:{} response:{}",
                        request.method(), request.url().toString(), request.body().toString(),
                        response.code(), response.body().string());
                throw new StatusException("status code error:" + response.code());
            }
            return response.body().string();
        }
    }
}
