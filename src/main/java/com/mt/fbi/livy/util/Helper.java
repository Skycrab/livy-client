package com.mt.fbi.livy.util;

/**
 * Created by yihaibo on 2019-04-22.
 */
public final class Helper {
    public static void sleep(long millis) {
        try{
            Thread.sleep(millis);
        }catch (InterruptedException e) {

        }
    }

    private Helper() {
    }
}
