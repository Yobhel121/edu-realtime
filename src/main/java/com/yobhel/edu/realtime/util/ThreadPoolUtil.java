package com.yobhel.edu.realtime.util;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 16:31
 **/
public class ThreadPoolUtil {

    private static ThreadPoolExecutor poolExecutor;

    public static ThreadPoolExecutor getInstance() {
        if (poolExecutor == null) {
            synchronized (ThreadPoolUtil.class) {
                if (poolExecutor == null) {
                    poolExecutor = new ThreadPoolExecutor(4, 20, 60 * 5, TimeUnit.SECONDS, new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE));
                }
            }
        }
        return poolExecutor;
    }
}
