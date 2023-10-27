package com.yobhel.edu.realtime.app.func;

import com.yobhel.edu.realtime.util.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 16:25
 **/
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {

    private String tableName;

    ThreadPoolExecutor executorService;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) throws Exception {
        executorService.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    String key = getKey(input);


                }catch (Exception e){
                    e.printStackTrace();
                    System.out.println("一步维度关联出错");
                }
            }
        });

    }
}
