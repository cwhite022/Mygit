package com.atguigu.day10;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/14 16:56
 * @Version 1.0
 */
public class Flink08_SQL_02 {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.1 创建表执行环境
       /* new EnvironmentSettings.Builder()
                .withBuiltInCatalogName()
                .build()*/ //给tableEnv设置一些参数
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);
        //转变为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0],
                    Long.parseLong(fields[1]),
                    Integer.parseInt(fields[2]));
        });

        //TODO 3.将流转化为动态表
        Table sensorTable = tableEnv.fromDataStream(waterSensorDS);

        //TODO 4.注册表
        tableEnv.createTemporaryView("sensor", sensorTable);

        //TODO 5. 使用FlinkSQL查询
//        Table resultTable = tableEnv.sqlQuery("select id,vc from sensor where id >= '1003'");
        Table resultTable = tableEnv.sqlQuery("select id,count(*) ct,sum(vc) sumVc from sensor group by id");

        resultTable.execute().print();
    }
}
