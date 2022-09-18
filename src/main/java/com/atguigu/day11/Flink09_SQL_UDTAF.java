package com.atguigu.day11;

import com.atguigu.day11.function.MyUDF;
import com.atguigu.day11.function.MyUDTAF;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

/**
 * @Author CZQ
 * @Date 2022/7/15 0:00
 * @Version 1.0
 */
public class Flink09_SQL_UDTAF {
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
        //TODO 3. 注册UDF
        tableEnv.createTemporaryFunction("my_udtaf", MyUDTAF.class);

        //TODO 4.使用UDF做查询并打印
   /*      tableEnv.from("sensor_pt")
                .groupBy($("myField"))
                .flatAggregate(call("Top2", $("value")).as("value", "rank"))
                .select($("myField"), $("value"), $("rank"));*/

/*        Table sensorTable = tableEnv.from("sensor_pt");
        sensorTable.groupBy($("id"))
                .flatAggregate(call("my_udtaf",$("vc")).as("v","rk"))
                .select($("id"),$("v"),$("rk"))
        .execute().print();*/

        //这里创建的表都是临时的，所以Catalog查看的都是临时的元数据
        tableEnv.executeSql("show tables").print();


    }
}
