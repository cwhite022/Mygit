package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/14 23:50
 * @Version 1.0
 */
public class Flink02_SQL_HopWindow {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //flinkSQL中kafka的主题分区一定要和并发保持一致
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.使用DDL方式读取Kafka数据创建动态表 注意提取事件时间或者处理时间

        // pt表
        tableEnv.executeSql("" +
                "CREATE TABLE sensor_pt ( " +
                "  `id` STRING, " +
                "  `ts` BIGINT, " +
                "  `vc` DOUBLE, " +
                "  `pt` AS PROCTIME() " +
                ") WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'test_flink_window', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = 'bigdata_0212', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'csv' " +
                ")");

        // rt表  AS TO_TIMESTAMP_LTZ(ts,0)
        tableEnv.executeSql("" +
                "CREATE TABLE sensor_rt ( " +
                "  `id` STRING, " +
                "  `ts` BIGINT, " +
                "  `vc` DOUBLE, " +
                "  `rt` AS to_timestamp(from_unixtime(ts,'yyyy-MM-dd HH:mm:ss')), " +
                "  WATERMARK FOR rt AS rt - INTERVAL '5' SECOND " +
                ") WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = 'test_flink_window', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = 'bigdata_0212', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'csv' " +
                ")");

        //TODO 3.分组、开窗、聚合
        tableEnv.sqlQuery("" +
                "select " +
                "    id, " +
                "    sum(vc), " +
                "    HOP_START(pt, INTERVAL '2' SECOND, INTERVAL '6' SECOND) stt, " +
                "    HOP_END(pt, INTERVAL '2' SECOND, INTERVAL '6' SECOND) edt " +
                "from sensor_pt " +
                "group by id, " +
                "HOP(pt, INTERVAL '2' SECOND, INTERVAL '6' SECOND)");

        tableEnv.sqlQuery("" +
                "select " +
                "    id, " +
                "    sum(vc), " +
                "    HOP_START(rt, INTERVAL '2' SECOND, INTERVAL '6' SECOND) stt, " +
                "    HOP_END(rt, INTERVAL '2' SECOND, INTERVAL '6' SECOND) edt " +
                "from sensor_rt " +
                "group by id, " +
                "HOP(rt, INTERVAL '2' SECOND, INTERVAL '6' SECOND)").execute().print();


    }
}
