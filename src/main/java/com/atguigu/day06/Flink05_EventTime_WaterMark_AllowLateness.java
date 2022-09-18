package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author CZQ
 * @Date 2022/7/11 16:37
 * @Version 1.0
 */
public class Flink05_EventTime_WaterMark_AllowLateness {

    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置生成WaterMark生成周期时间
        env.getConfig().setAutoWatermarkInterval(200);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将读过来的数据转为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream = streamSource.map(new MapFunction<String,
                WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        //TODO 4.设置WaterMark forMonotonousTimestamps他是一个泛型方法，所以前面要加上处理的泛型字段

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator =
                waterSensorDStream.assignTimestampsAndWatermarks(
                        // 允许设置固定延迟的WaterMark
                WatermarkStrategy
                        // 当传入时间为5s时候 waterMark = 5-3-1ms
                        .forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                return new MyWaterMarkGenator(Duration.ofSeconds(3));
                            }
                        })
                        // 指定哪个字段作为时间字段
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {

                                return element.getTs() * 1000;
                            }
                        })
        );

        //5.将相同id的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorSingleOutputStreamOperator.keyBy("id");

        //TODO 6. 开启一个基于 事件时间会话窗口，会话间隔3s
        WindowedStream<WaterSensor, Tuple, TimeWindow> window =
                keyedStream.window(EventTimeSessionWindows.withGap(Time.seconds(3)));

        window. process(new ProcessWindowFunction<WaterSensor, String, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple key, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                String msg = "当前key: " + key
                        + "窗口: [" + context.window().getStart() / 1000 + "," + context.window().getEnd()/1000 + ") 一共有 "
                        + elements.spliterator().estimateSize() + "条数据 ";
                out.collect(msg);
            }
        })
                .print();
        env.execute();

    }
    public static class MyWaterMarkGenator implements WatermarkGenerator<WaterSensor>{


        /** 到目前为止遇到的最大时间戳. */
        private long maxTimestamp;

        /** 该WWatermark所假定的最大乱序程度（等待时间）. */
        private  long outOfOrdernessMillis;
        public MyWaterMarkGenator(Duration maxOutOfOrderness) {


            this.outOfOrdernessMillis = maxOutOfOrderness.toMillis();

            // watermark的最小值为 Lond 的最小值.
            this.maxTimestamp = Long.MIN_VALUE + outOfOrdernessMillis + 1;
        }

        //间歇性调用，来一条数据调用一次
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
            System.out.println("WaterMark:"+(maxTimestamp - outOfOrdernessMillis - 1));
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
        }

        //周期性调用 ，默认每隔200ms调用一次
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
 /*           System.out.println("WaterMark:"+(maxTimestamp - outOfOrdernessMillis - 1));
            output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));*/
        }
    }
}
