package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Author CZQ
 * @Date 2022/7/12 16:31
 * @Version 1.0
 */
public class Flink07_Time_Exec_With_KeyedState {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.将数据转为WaterSensor
        SingleOutputStreamOperator<WaterSensor> waterSensorDStream =
                streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });

        //4.将相同id的数据聚合到一块
        KeyedStream<WaterSensor, String> keyedStream = waterSensorDStream.keyBy(r -> r.getId());

        //5.处理需求
        /**
         *每一个5s内的第一条数据来的时候先注册一个5s的定时器，如果水位一直都是处于上升状态则触发定时器，如果5s内有水位没有上升，则删除定时器，重新定时
         *     所以就要判断当数据来的时候定时器有没有注册（可以把懂事起的定时时间保存起来，以此来判断有没有注册定时器），如果有注册的话这个数据就不是5s内的第一条数据，如果没有注册定时器的话，那么就是5s
         *     内的第一条数据，需要注册定时器 。
         *     需要判断水位有没有上升，从而来决定当前这个定时器是否失效，如果当前来的水位大于上一次的水位（可以把上一次的水位保存起来，然后当前水位做对比，并更新），则意味着上升，什么也不做，
         *     等待定时器触发报警即可，如果当前来的水位小于等于上一次的水位，则需要删除当前的定时器，不让报警，然后再吓一跳
         */
        SingleOutputStreamOperator<String> process = keyedStream.process(new KeyedProcessFunction<String, WaterSensor
                , String>() {
            //保存上一次水位
/*            private Integer lastVc = Integer.MIN_VALUE;*/
            private ValueState<Integer> lastVc;

            //保存定时器时间
           /* private Long timer = Long.MIN_VALUE;*/
            private ValueState<Long> timer;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVc=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVc", Integer.class,
                        Integer.MIN_VALUE));
                timer=getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer", Long.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {
                //1.判断水位是否上升
              /*  if (value.getVc() > lastVc) {*/
                if (value.getVc() > lastVc.value()) {

                    //2.水位上升的情况下判断定时器是否被注册，如果没有注册则需要注册一个定时器，
                    // 同时也是在判断这个数据是不是5s内的第一条数据
                   /* if (timer == Long.MIN_VALUE) {*/
                    if (timer.value() == null) {
                        //定时器没有注册
                        //3.注册定时器（基于处理时间）
                       /* timer = ctx.timerService().currentProcessingTime() + 5000;*/
                        timer.update( ctx.timerService().currentProcessingTime() + 5000);
                        System.out.println("注册定时器"+ctx.timerService().currentProcessingTime()+ctx.getCurrentKey());
//                        ctx.timerService().registerProcessingTimeTimer(timer);
                        ctx.timerService().registerProcessingTimeTimer(timer.value());

                    }

                } else {
                    //4.如果水位没有上升
                    //5.判断有没有注册定时器
//                    if (timer != Long.MIN_VALUE) {
                    if (timer.value() != null ) {
                        //意味着之前有注册定时器
                        //6.删除之前的定时器
//                        ctx.timerService().deleteEventTimeTimer(timer);
                        ctx.timerService().deleteEventTimeTimer(timer.value());
                        System.out.println("删除定时器："+ctx.getCurrentKey());

                        //7.为了方便下一次注册定时器，需要重置定时器时间
//                        timer = Long.MIN_VALUE;
                       /* lastVc = Integer.MIN_VALUE;*/

                        //清空状态
                        timer.clear();
                    }
                }
                //8.无论如何都要去更新水位值，以便下一次做对比
//                lastVc = value.getVc();
                lastVc.update(value.getVc());
                out.collect(value.toString());
            }

            /**
             * 定时器触发了怎么办？
             * 1.在侧输出流中发出报警信息
             * 2.重置定时器时间，以便下一个5s重新监控
             *
             *
             * @param timestamp
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                //9.将报警信息放在侧输出流中
                ctx.output(new OutputTag<String>("output") {
                }, ctx.getCurrentKey()+"警告！水位5s内连续上升");

                //10.重置定时器时间
//                timer = Long.MIN_VALUE;
                timer.clear();
            }
        });
        process.print("主流");
        process.getSideOutput(new OutputTag<String>("output"){}).print("测流");
        env.execute();
    }
}
