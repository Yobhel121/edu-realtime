package com.yobhel.edu.realtime.bean;

import lombok.Data;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 16:51
 **/
@Data
public class DwdTableProcess {

    // 来源表
    String sourceTable;

    // 操作类型
    String sourceType;

    // 输出表
    String sinkTable;

    // 输出字段
    String sinkColumns;
}
