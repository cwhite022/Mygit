package com.atguigu.day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @Author CZQ
 * @Date 2022/7/8 20:15
 * @Version 1.0
 */
public class Flink08_TimeWindow_Tumbling_WindowFun_AggFun {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据组成Tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> map = streamSource.map(new MapFunction<String,
                Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {

                return Tuple2.of(value, 1);
            }
        });
        //4.将相同的单词聚合到一块
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = map.keyBy(0);

        // TODO 5. 开启一个基于时间的滚动窗口,窗口大小为10s
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window = keyedStream.window(
                TumblingProcessingTimeWindows.of(Time.seconds(10)));

        // TODO 6.对窗口中的数据进行累加计算(ReduceFun) 第一条数据不进入Reduce
     window.aggregate(new AggregateFunction<Tuple2<String, Integer>, Integer, Integer>() {
         /**
          * 创建累加器，每个窗口都创建一个累加器，分Key
          * @return
          */
         @Override
         public Integer createAccumulator() {
             System.out.println("创建累加器");
             return 0;
         }

         /**
          * 累加操作
          * @param value
          * @param accumulator
          * @return
          */
         @Override
         public Integer add(Tuple2<String, Integer> value, Integer accumulator) {
             System.out.println("累加操作");
             return value.f1+accumulator;
         }

         /**
          * 从累加器中获取结果
          * @param accumulator
          * @return
          */
         @Override
         public Integer getResult(Integer accumulator) {
             System.out.println("获取结果");
             return accumulator;
         }

         /**
          * 合并累加器，只有在会话窗口的特殊情况下才会调用
          * @param a
          * @param b
          * @return
          */
         @Override
         public Integer merge(Integer a, Integer b) {
             System.out.println("合并累加器");
             return a+b;
         }
     }).print();

        env.execute();

    }
}
