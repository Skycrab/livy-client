package com.mt.fbi.livy.util;

import java.io.IOException;

/**
 * Created by yihaibo on 2019-04-19.
 */
public interface RestClient {
    String get(String url) throws IOException;
    String post(String url, Object data) throws IOException;
    void delete(String url) throws IOException;
}
