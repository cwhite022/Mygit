package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/6/28 20:21
 * @Version 1.0
 */
public class Flink02_Stream_WordCount {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //将并行度设置为1
        env.setParallelism(1);

        //2.从文件读取数据
        DataStreamSource<String> streamSource = env.readTextFile("input/word.txt");

        /**
         * 在SPark中对数据的处理，首先flatMap（按照空格切分，然后组成Tuple2元组（word,1））
         * 其次ReduceByKey（1.先将相同的单词聚合到一块 2.在做累加）
         * 最后把结果输出到控制台
         */
        //3.按照空格切分，然后组成Tuple2元组（word,1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDSteam =
                streamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                //1.按照空格切分
                String[] words = value.split(" ");
                for (String word : words) {
                    //返回tuple2元组
                    out.collect(Tuple2.of(word, 1));

                }
            }
        });
        //4.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, String> keyedStream =
                wordToOneDSteam.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {

            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //5.做累加计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //6.打印到控制台
        result.print();

        //7.执行代码（不然不会打印出东西）相当于一个提交JOB的过程
        env.execute();
    }
}
