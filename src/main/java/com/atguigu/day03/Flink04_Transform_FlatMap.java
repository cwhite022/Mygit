package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/7/6 14:49
 * @Version 1.0
 */
public class Flink04_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.使用flatMap将读过来的数据按照空格进行切分切出每一个单词
        streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word); // 对于Map这种一进一出的都是有返回值的，对于FlatMap这种一进多出的都是用collect把数据发送出去 给下游
                }
            }
        }).print();
        env.execute();
    }
    //FlatMapFunction也有富函数版本，用法和Map的富函数一样
    public static class MyFlatMap extends RichFlatMapFunction<String,String> {

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void flatMap(String value, Collector<String> out) throws Exception {

        }
    }
}
