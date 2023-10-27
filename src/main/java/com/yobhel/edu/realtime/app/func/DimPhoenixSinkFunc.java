package com.yobhel.edu.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.yobhel.edu.realtime.util.PhoenixUtil;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 17:14
 **/
public class DimPhoenixSinkFunc implements SinkFunction<JSONObject> {

    @Override
    public void invoke(JSONObject jsonObject, Context context) throws Exception {
        String sinkTable = jsonObject.getString("sink_table");
        String type = jsonObject.getString("type");
        String id = jsonObject.getString("id");
        jsonObject.remove("sink_table");
        jsonObject.remove("type");

        PhoenixUtil.executeDML(sinkTable,jsonObject);

//        if("update".equals(type)){
//            DimUtil.deleteCached(sinkTable,id);
//        }

    }
}
