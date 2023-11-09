package com.yobhel.edu.realtime.app.func;

import com.yobhel.edu.realtime.util.KeyWordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.ArrayList;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-11-09 16:52
 **/
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class KeyWordUDTF  extends TableFunction<Row> {
    public void eval(String text) {
        ArrayList<String> analyze = KeyWordUtil.analyze(text);
        for (String s : analyze) {
            collect(Row.of(s));
        }
    }
}

