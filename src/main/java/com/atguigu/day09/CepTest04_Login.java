package com.atguigu.day09;

import com.atguigu.bean.LoginEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author CZQ
 * @Date 2022/7/14 6:16
 * @Version 1.0
 */
public class CepTest04_Login {
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

        //TODO 5.定义模式序列
        Pattern<LoginEvent, LoginEvent> pattern =
                Pattern.<LoginEvent>begin("start").where(new SimpleCondition<LoginEvent>() {
            @Override
            public boolean filter(LoginEvent value) throws Exception {
                return "fail".equals(value.getEventType());
            }
        })
              /*  .times(2)*/ // 连续登陆失败两次
                        .timesOrMore(2) // 连续登陆失败两次及两次以上
                .consecutive()
                .within(Time.seconds(2));

        //TODO 6.将模式序列作用到流上
        PatternStream<LoginEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件
        SingleOutputStreamOperator<String> selectDS =
                patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> pattern) throws Exception {
                List<LoginEvent> events = pattern.get("start");
                LoginEvent start = events.get(0);
                LoginEvent end = events.get(1);

                return start.getUserId() + " 在 " + start.getEventTime() + " 到 " + end.getEventTime() + " 之间连续登录失败2次 ";
            }
        });

        //TODO 8. 打印
        selectDS.print(">>>>>>");


        //TODO 9.启动任务
        env.execute();
    }
}
