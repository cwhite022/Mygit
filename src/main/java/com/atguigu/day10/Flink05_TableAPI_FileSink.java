package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author CZQ
 * @Date 2022/7/14 22:15
 * @Version 1.0
 */
public class Flink05_TableAPI_FileSink {
    public static void main(String[] args) {
        // TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //TODO 2.读取文本数据创建表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
        //通过定义创建一个读连接器 放在From的位置就是读连接器 是source  Table sensorTable = tableEnv.from("sensor");
        tableEnv.connect(new FileSystem().path("input/sensor-sql.txt"))
                .withFormat(new Csv().fieldDelimiter(',').lineDelimiter("\n"))
                .withSchema(schema)
                .createTemporaryTable("sensor");

        //TODO 3. 执行查询并写出
        Table sensorTable = tableEnv.from("sensor");
        Table whereTable = sensorTable.where($("id").isGreaterOrEqual("sensor_2"));
        //通过定义创建一个写连接器 放在ExecuterInsert的位置便是写连接器  是sink  whereTable.executeInsert("sensor2");
        tableEnv.connect(new FileSystem().path("input/sensor-sql2.txt"))
                .withFormat(new Csv().fieldDelimiter('|'))
                .withSchema(schema)
                .createTemporaryTable("sensor2");
        whereTable.executeInsert("sensor2");

    }
}
