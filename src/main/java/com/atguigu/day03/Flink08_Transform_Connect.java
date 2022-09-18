package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @Author CZQ
 * @Date 2022/7/6 16:34
 * @Version 1.0
 * 只能2个流进行Connect,不能有第三个流参与
 */
public class Flink08_Transform_Connect {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.获取字母流
        DataStreamSource<String> strStreamSource = env.fromElements("a", "b", "c", "d");
        //数字流
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(1, 2, 3, 4);

        // TODO 3.使用Connect连接两条流
        ConnectedStreams<String, Integer> connect = strStreamSource.connect(integerDataStreamSource);

        //4.对连接后的流做map操作
        SingleOutputStreamOperator<String> map = connect.map(new CoMapFunction<String, Integer, String>() {
            @Override
            public String map1(String value) throws Exception {
                return value + "cls";
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * 1000 + "";
            }
        });
        map.print();
        env.execute();

    }
}
