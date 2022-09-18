package com.atguigu.day10;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author CZQ
 * @Date 2022/7/14 21:17
 * @Version 1.0
 */
public class Flink06_TableAPI_KafkaSink {
    public static void main(String[] args) {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //TODO 2. 读取Kafka数据创建表
        Schema schema = new Schema()
                .field("id", DataTypes.STRING())
                .field("ts", DataTypes.BIGINT())
                .field("vc", DataTypes.INT());
       tableEnv.connect(new Kafka()
                       .version("universal")
       .topic("test")
               //配置以开始从所有分区的最新偏移中读取(消费最新的数据)
       .startFromLatest()
            //为Kafka消费者添加配置属性
       .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
               //添加消费者组
       .property(ConsumerConfig.GROUP_ID_CONFIG, "bigdata_0212"))
               //用json格式去表达表中的数据
       .withFormat(new Json())
               //指定结果表模式
       .withSchema(schema)
               //创建一个临时表
       .createTemporaryTable("sensor");


        //TODO 3.执行查询并写出到Kafka
        Table sensorTable = tableEnv.from("sensor");
        Table whereTable = sensorTable.where($("id").isGreaterOrEqual("sensor_2"));

        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("test1")
                //为Kafka生产者添加配置属性
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092"))
                //用json格式去表达表中的数据
                .withFormat(new Json())
                //指定结果表模式
                .withSchema(schema)
                //创建一个临时表
                .createTemporaryTable("sensor2");
        //往sensor2中去写数据
        whereTable.executeInsert("sensor2");
    }
}
