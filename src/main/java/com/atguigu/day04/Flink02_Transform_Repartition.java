package com.atguigu.day04;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/7 9:56
 * @Version 1.0
 */
public class Flink02_Transform_Repartition {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(4);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //3.做Map操作
        SingleOutputStreamOperator<String> map = streamSource.map(r -> r).setParallelism(2);

        //4.对流重新分区
        KeyedStream<String, String> keyedStream = map.keyBy(r -> r);
        DataStream<String> shuffle = map.shuffle();
        DataStream<String> rebalance = map.rebalance();
        DataStream<String> rescale = map.rescale();

        map.print("原始数据").setParallelism(2);
        keyedStream.print("KeyBy");
        shuffle.print("Shuffle");
        rebalance.print("Rebalance");
        rescale.print("Rescalse");

        env.execute();
    }
}
