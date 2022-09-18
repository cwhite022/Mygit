package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/14 23:45
 * @Version 1.0
 */
public class Flink11_SQL_PtWithDDL {
    public static void main(String[] args) {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式创建KafkaSource或者Sink 创建第一个表
        tableEnv.executeSql("" +
                "CREATE TABLE sensor1 (   " +
                "  `id` STRING,   " +
                "  `ts` BIGINT,   " +
                "  `vc` DOUBLE," +
                "   `pt` AS PROCTIME() " +
                ") WITH (   " +
                "  'connector' = 'kafka',   " +
                "  'topic' = 'test',   " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',   " +
                "  'properties.group.id' = 'bigdata_0212',   " +
                "  'scan.startup.mode' = 'latest-offset',   " +
                "  'format' = 'csv'   " +
                ")");

        //TODO 3. 查询数据并打印
        tableEnv.sqlQuery("select * from sensor1").execute().print();

    }
}
