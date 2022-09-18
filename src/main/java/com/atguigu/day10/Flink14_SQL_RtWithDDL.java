package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.ZoneId;

/**
 * @Author CZQ
 * @Date 2022/7/14 23:45
 * @Version 1.0
 */
public class Flink14_SQL_RtWithDDL {
    public static void main(String[] args) {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        TableConfig tableConfig = tableEnv.getConfig();
/*        tableConfig.setLocalTimeZone(ZoneId.of("UTC+0"));*/
        tableConfig.setLocalTimeZone(ZoneId.of("Asia/Shanghai"));

        //TODO 2.使用DDL方式创建KafkaSource或者Sink 创建第一个表
        tableEnv.executeSql("" +
                "CREATE TABLE sensor1 (  " +
                "  `id` STRING,  " +
                "  `ts` BIGINT,  " +
                "  `vc` DOUBLE,  " +
                "  `rt` AS TO_TIMESTAMP_LTZ(ts,3),  " +
                "  WATERMARK FOR rt AS rt - INTERVAL '5' SECOND  " +
                ") WITH (  " +
                "  'connector' = 'kafka',  " +
                "  'topic' = 'test',  " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092',  " +
                "  'properties.group.id' = 'bigdata_0212',  " +
                "  'scan.startup.mode' = 'latest-offset',  " +
                "  'format' = 'csv'  " +
                ")");

        //TODO 3. 查询数据并打印
        tableEnv.sqlQuery("select * from sensor1").execute().print();

    }
}
