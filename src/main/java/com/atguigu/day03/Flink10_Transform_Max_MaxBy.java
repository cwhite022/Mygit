package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/6 10:58
 * @Version 1.0
 */
public class Flink10_Transform_Max_MaxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        // 读文件并不一定是有界数据，也有可能是无界的
//        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");
        //TODO 3. 使用Map将从端口读出来的字符串转为WaterSensor(JAVABean)
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //  4.使用keyby将相同id的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        // TODO 5.使用简单滚动聚合算子
//        SingleOutputStreamOperator<WaterSensor> max = keyedStream.max("vc");
        SingleOutputStreamOperator<WaterSensor> maxBytrue = keyedStream.maxBy("vc", true);
        maxBytrue.print();


        env.execute();
    }


}
