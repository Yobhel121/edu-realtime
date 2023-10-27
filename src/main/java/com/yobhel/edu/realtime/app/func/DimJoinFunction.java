package com.yobhel.edu.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 16:25
 **/
public interface DimJoinFunction<T> {
    // 关联方法
    void join(T obj, JSONObject jsonObject) throws Exception;

    // 获取关联字段
    String getKey(T obj);

}
