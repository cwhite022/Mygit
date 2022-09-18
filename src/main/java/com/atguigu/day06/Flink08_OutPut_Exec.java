package com.atguigu.day06;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Author CZQ
 * @Date 2022/7/11 16:37
 * @Version 1.0
 */
public class Flink08_OutPut_Exec {
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
        });

        //4.将相同id的数据聚合到一块
        KeyedStream<WaterSensor, Tuple> keyedStream = waterSensorDStream.keyBy("ts");

        //5. 采集监控传感器水位值，将水位值高于5cm的值输出到side output
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<Tuple, WaterSensor,
                String>() {
            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                if (value.getVc() > 5) {
                    // 水位值高于5cm的值输出到side Output中
                    ctx.output(new OutputTag<String>("output") {
                    }, value.toString());
                } else if (value.getVc()<3){
                   ctx.output(new OutputTag<String>("low3"){},value.toString());
                }
                out.collect(value.toString());
            }
        });
        process.print("主流");
        process.getSideOutput(new OutputTag<String>("output"){
        }).print("高于5cm");
        process.getSideOutput(new OutputTag<String>("low3"){
        }).print("小于3cm");

        env.execute();


    }
}
