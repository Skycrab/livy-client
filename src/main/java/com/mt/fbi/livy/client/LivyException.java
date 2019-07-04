package com.mt.fbi.livy.client;

/**
 * Created by yihaibo on 2019-04-19.
 */
public class LivyException extends Exception {
    public LivyException() {
    }

    public LivyException(String message) {
        super(message);
    }

    public LivyException(String message, Throwable cause) {
        super(message, cause);
    }

    public LivyException(Throwable cause) {
        super(cause);
    }

    public LivyException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
