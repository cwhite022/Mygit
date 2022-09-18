package com.atguigu.day04;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/6/28 21:00
 * @Version 1.0
 */
public class Flink08_WordCount_RuntimeMode {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 设置运行时执行模式
        // 流的
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        // 批的 如果是1.12版本的话，批处理是统计不出来个数为1 的数据(BUG) 1.13版本修复了
    // env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        //自动的
      //  env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        //将并行度设置为1 方便观察
        env.setParallelism(1);

        //2.读取无界数据
     //   DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");
        //3.按照空格切分，然后组成Tuple2元组（word,1）
        SingleOutputStreamOperator<String> wordDStream =
                streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                //遍历数组取出每一个单词
                for (String word : words) {
                    out.collect(word);
                }

            }
        });

        //4.将每一个单词组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDStream = wordDStream.map(new MapFunction<String,
                Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        });
        //5.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDStream.keyBy(0);

        //6.做累加操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);
        //7.打印到控制台
        result.print("累加结果");
        env.execute();
    }
}
