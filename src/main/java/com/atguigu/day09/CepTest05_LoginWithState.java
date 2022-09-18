package com.atguigu.day09;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author CZQ
 * @Date 2022/7/14 6:16
 * @Version 1.0
 */
public class CepTest05_LoginWithState {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取登录日志
        DataStreamSource<String> textFileDS = env.readTextFile("input/LoginLog.csv");

        //TODO 3.将数据转化为JavaBean对象
        SingleOutputStreamOperator<LoginEvent> loginEventDS = textFileDS.map(line -> {
            String[] fields = line.split(",");
            return new LoginEvent(Long.parseLong(fields[0]),
                    fields[1], fields[2], Long.parseLong(fields[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<LoginEvent>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                // 指定哪个字段作为时间字段
                .withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        //TODO 4.按照User_id进行分组
        KeyedStream<LoginEvent, Long> keyedStream = loginEventDS.keyBy(LoginEvent::getUserId);

        // TODO 5. 使用状态编程的方式获取连续的失败登录
        SingleOutputStreamOperator<String> outputStreamOperator =
                keyedStream.flatMap(new RichFlatMapFunction<LoginEvent, String>() {

            private ValueState<LoginEvent> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //状态初始化
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<LoginEvent>("value-state",
                        LoginEvent.class));
            }

            @Override
            public void flatMap(LoginEvent value, Collector<String> out) throws Exception {
                //判断当前数据状态是成功还是失败
                if ("fail".equals(value.getEventType())) {
                    //取出状态中的数据
                    LoginEvent lastValues = valueState.value();

                    if (lastValues != null && value.getEventTime() - lastValues.getEventTime() <= 2) {
                        out.collect(lastValues.getUserId() + " 在 " + lastValues.getEventTime() + " 到 " + value.getEventTime() + " 之间连续登录失败2次 ")
                        ;
                    }

                    valueState.update(value);
                } else {
                    valueState.clear();

                }

            }
        });

        //TODO 8. 打印
        outputStreamOperator.print(">>>>>>");



        //TODO 9.启动任务
        env.execute();
    }
}
