package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/14 23:41
 * @Version 1.0
 */
public class Flink10_SQL_Kafka2Kafka_Ex {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式创建KafkaSource或者Sink 创建第一个表
        tableEnv.executeSql("" +
                "CREATE TABLE sensor1 (   " +
                "  `id` STRING,   " +
                "  `ts` BIGINT,   " +
                "  `vc` DOUBLE   " +
                ") WITH (   " +
                "  'connector' = 'kafka',   " +
                "  'topic' = 'test',   " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',   " +
                "  'properties.group.id' = 'bigdata_0212',   " +
                "  'scan.startup.mode' = 'latest-offset',   " +
                "  'format' = 'csv'   " +
                ")");


        //TODO 3.读取Kafka数据转换为流做打印
        Table resultTable = tableEnv.sqlQuery("select id,sum(vc) vc from sensor1 group by id");
        //注册一下过滤后的表
        tableEnv.createTemporaryView("result_table", resultTable);

        //TODO 4.使用DDL方式创建KafkaSink 创建第二个表
        tableEnv.executeSql("" +
                "CREATE TABLE sensor2 ( " +
                "  `id` STRING, " +
                "  `vc` DOUBLE,  " +
                " PRIMARY KEY (id) NOT ENFORCED " +
                ") WITH ( " +
                "  'connector' = 'upsert-kafka',     " +
                "  'topic' = 'test1',     " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',     " +
                "  'key.format' = 'json',     " +
                "  'value.format' = 'json'     " +
                ")");

        //TODO 5.将数据写出
        tableEnv.executeSql("insert into sensor2 select * from result_table");


    }
}
