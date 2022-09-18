package com.atguigu.day11;


import com.atguigu.day11.function.MyUDTF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/15 0:00
 * @Version 1.0
 */
public class Flink07_SQL_UDTF {
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
        //TODO 3. 注册UDTF
        tableEnv.createTemporaryFunction("my_udtf", MyUDTF.class);

        //TODO 4.使用UDTF做查询并打印
        tableEnv.sqlQuery("" +
                "SELECT " +
                "id," +
                "newWord, " +
                "newLength " +
                "FROM sensor_pt " +
                "LEFT JOIN " +
                "LATERAL TABLE(my_udtf(id)) AS t(newWord,newLength) ON TRUE")
                .execute()
                .print();



    }
}
