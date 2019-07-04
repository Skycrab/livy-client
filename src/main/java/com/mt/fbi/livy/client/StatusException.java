package com.mt.fbi.livy.client;

/**
 * Created by yihaibo on 2019-04-19.
 */
public class StatusException extends RuntimeException {
    public StatusException() {
    }

    public StatusException(String message) {
        super(message);
    }

    public StatusException(String message, Throwable cause) {
        super(message, cause);
    }

    public StatusException(Throwable cause) {
        super(cause);
    }

    public StatusException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
