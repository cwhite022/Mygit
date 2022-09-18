package com.atguigu.day04;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @Author CZQ
 * @Date 2022/7/7 11:50
 * @Version 1.0
 */
public class Flink03_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.先将数据转为JavaBean（Warnsenor）,然后再转为Json字符串
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {

            @Override
            public String map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return JSONObject.toJSONString(waterSensor);
            }
        });
        //TODO 4.将数据写入Kafka
        map.addSink(new FlinkKafkaProducer<String>("hadoop102:9092", "topic_sensor",
                new SimpleStringSchema()));
        env.execute();
    }
}
