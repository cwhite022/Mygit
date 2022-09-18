package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @Author CZQ
 * @Date 2022/7/6 0:11
 * @Version 1.0
 */
public class Flink02_Source_Collection {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //TODO 2.从集合中获取数据
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6);
//        DataStreamSource<Integer> streamSource = env.fromCollection(list);
        // 从元素中获取数据
        DataStreamSource<Integer> streamSource = env.fromElements(1, 2, 3, 4, 5, 6);

        streamSource.print();
        env.execute();
    }
}
