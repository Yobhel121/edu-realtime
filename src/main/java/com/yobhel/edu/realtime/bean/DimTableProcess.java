package com.yobhel.edu.realtime.bean;

import lombok.Data;

/**
 * 类描述：TODO
 *
 * @author yzm
 * @date 2023-10-23 16:23
 **/
@Data
public class DimTableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
