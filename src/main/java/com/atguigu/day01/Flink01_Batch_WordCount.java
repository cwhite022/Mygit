package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/6/28 19:38
 * @Version 1.0
 */
public class Flink01_Batch_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2.从文件读取数据
        DataSource<String> dataSource = env.readTextFile("input/word.txt");

        /**
         * 在SPark中对数据的处理，首先flatMap（按照空格切分，然后组成Tuple2元组（word,1））
         * 其次ReduceByKey（1.先将相同的单词聚合到一块 2.在做累加）
         * 最后把结果输出到控制台
         */
        //3.按照空格切分，然后组成Tuple2元组（word,1）
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOne =
                dataSource.flatMap(new MyFlatMap());

        //4.由于没有reduceByKey，所以将相同的单词聚合到一块
        UnsortedGrouping<Tuple2<String, Integer>> groupBy = wordToOne.groupBy(0);

        //5.将单词个数做累加
        AggregateOperator<Tuple2<String, Integer>> result = groupBy.sum(1);
        result.print();
    }
    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        /**
         *
         * @param value 输入的数据
         * @param out 采集器，将数据发送到下游
         * @throws Exception
         */

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按照空格切分
            String[] words = value.split(" ");
            //遍历每一个单词
            for (String word : words) {
//                out.collect(new Tuple2<>(word,1));
                out.collect(Tuple2.of(word,1));
            }

        }
    }
}
