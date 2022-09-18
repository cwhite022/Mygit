package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author CZQ
 * @Date 2022/7/11 16:37
 * @Version 1.0
 */
public class Flink10_EventTime_Timer {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs()*1000;
                            }
                        })
                )
                ;

        //4.将相同id的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("ts");

        //5. 注册基于处理时间的定时器，定时时间为5s,5s后定时器触发打印一句话
        //定时器在用的时候只能在keyBy之后使用
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor,
                String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //注册一个基于事件时间的定时器  ctx.timestamp() 便是数据的事件时间
                System.out.println("注册定时器："+ctx.timestamp());
                ctx.timerService().registerEventTimeTimer(ctx.timestamp()+5000);

            }

            /**
             * 达到定时器时间后，触发这个方法
             * @param timestamp 定时器触发的时间
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
               //打印的是触发器的时间 ctx.timestamp6000
                System.out.println("ctx.timestamp"+ctx.timestamp());
                System.out.println("timestamp"+timestamp);
                out.collect("定时器被触发++++++");
            }
        });

        process.print();


        env.execute();


    }
}
