    package com.atguigu.day05;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

    /**
 * @Author CZQ
 * @Date 2022/7/8 20:15
 * @Version 1.0
 */
public class Flink09_TimeWindow_Tumbling_WindowFun_Process {
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

        // TODO 6.对窗口中的数据进行累加计算(Process)
        // process的功能更全更多 所以一般用process不用apply
        window.process(new ProcessWindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow>() {
            private  Integer count=0;

            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Integer> out) throws Exception {
                System.out.println("process....");
               // context.window().可以获得跟Apply一样的功能
                for (Tuple2<String, Integer> element : elements) {
                    count++;
                }
                out.collect(count);
            }
        }).print();



        env.execute();

    }
}
