package com.atguigu.day09;

import com.atguigu.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * @Author CZQ
 * @Date 2022/7/14 15:18
 * @Version 1.0
 */
public class CepTest06_ORderPay {
    public static void main(String[] args) throws Exception {
        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2.读取订单支付数据
        DataStreamSource<String> textFileDS = env.readTextFile("input/OrderLog.csv");

        //TODO 3.将数据转换为JavaBean，提前事件时间生成WaterMark
        SingleOutputStreamOperator<OrderEvent> orderEventDS = textFileDS.map(line -> {
            String[] split = line.split(",");
            return new OrderEvent(Long.parseLong(split[0])
                    , split[1]
                    , split[2]
                    , Long.parseLong(split[3]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy
                .<OrderEvent>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<OrderEvent>() {
                    @Override
                    public long extractTimestamp(OrderEvent element, long recordTimestamp) {
                        return element.getEventTime() * 1000L;
                    }
                }));

        //TODO 4.按照订单ID进行分组
        KeyedStream<OrderEvent, Long> keyedStream = orderEventDS.keyBy(OrderEvent::getOrderId);

        //TODO 5.定义模式序列
        Pattern<OrderEvent, OrderEvent> pattern =
                Pattern.<OrderEvent>begin("start").where(new SimpleCondition<OrderEvent>() {
            //获取事件类型
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "create".equals(value.getEventType());
            }
        }).next("next").where(new SimpleCondition<OrderEvent>() {
            @Override
            public boolean filter(OrderEvent value) throws Exception {
                return "pay".equals(value.getEventType());
            }
        }).within(Time.minutes(15));

        //TODO 6.将模式序列作用到流上
        PatternStream<OrderEvent> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提前事件（包含超时事件）
        OutputTag<String> outputTag = new OutputTag<String>("TimeOut") {
        };
        SingleOutputStreamOperator<Tuple2<OrderEvent, OrderEvent>> selectDS = patternStream.select(outputTag,
                new PatternTimeoutFunction<OrderEvent, String>() {

            @Override
            public String timeout(Map<String, List<OrderEvent>> pattern, long timeoutTimestamp) throws Exception {
                OrderEvent start = pattern.get("start").get(0);
                return start.getOrderId() + ":" + start.getEventTime();
            }
        }, new PatternSelectFunction<OrderEvent, Tuple2<OrderEvent, OrderEvent>>() {

            @Override
            public Tuple2<OrderEvent, OrderEvent> select(Map<String, List<OrderEvent>> pattern) throws Exception {
                OrderEvent start = pattern.get("start").get(0);
                OrderEvent next = pattern.get("next").get(0);
                System.out.println("=====>>>>"+start.getOrderId()+":"+(next.getEventTime()-start.getEventTime()));
                return new Tuple2<OrderEvent, OrderEvent>(start, next);
            }
        });

        //TODO 8.打印数据
        selectDS.getSideOutput(outputTag).print("TimeOut>>>>>>>>>>>>");
        selectDS.print("Select>>>>>>>>>>>>>>>");

        //TODO 9.启动任务
        env.execute();
    }
}
