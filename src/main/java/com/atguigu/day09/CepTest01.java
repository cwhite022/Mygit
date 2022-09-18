package com.atguigu.day09;

import com.atguigu.bean.WaterSensor;
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

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author CZQ
 * @Date 2022/7/14 4:42
 * @Version 1.0
 *
 * 需求：以WaterSensor 数据
 *      如果同一个传感器连续3条水位线超过30 ，则输出报警信息(那个传感器水位线连续3次超过30)
 *  CEP编程 3 步：
 *      1.定义模式序列（规则）
 *      2.将模式顺序作用到流上
 *      3.提取事件（注意：如果使用了Within关键字，需要考虑是否提取超时事件---->超时事件是放在测输出流的）
 */
public class CepTest01 {
    public static void main(String[] args) throws Exception {
        //TODO 1. 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //TODO 2. 读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //TODO 3.将数据转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = socketTextStream.map(line -> {
            String[] fields = line.split(",");
            return new WaterSensor(fields[0], Long.parseLong(fields[1]), Integer.parseInt(fields[2]));
        });
        SingleOutputStreamOperator<WaterSensor> waterSensorWithWmDS =
                waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }));
  /*      SingleOutputStreamOperator<WaterSensor> waterSensorWithWmDS =
                waterSensorDS.assignTimestampsAndWatermarks(WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                            @Override
                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                return element.getTs() * 1000L;
                            }
                        }));*/
        // TODO 4.按照传感器ID分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorWithWmDS.keyBy(WaterSensor::getId);

        //TODO 5.定义模式序列（规则）
        Pattern<WaterSensor, WaterSensor> pattern =
                Pattern.<WaterSensor>begin("start").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return value.getVc() > 30;
            }
        }).next("n1").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return value.getVc() > 30;
            }
        }).next("n2").where(new SimpleCondition<WaterSensor>() {
            @Override
            public boolean filter(WaterSensor value) throws Exception {
                return value.getVc() > 30;
            }
        });

        //TODO 6.将模式顺序作用到流上
        PatternStream<WaterSensor> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取事件
        SingleOutputStreamOperator<String> selectDS = patternStream.select(new PatternSelectFunction<WaterSensor,
                String>() {
            @Override
            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                // 提取3条数据并打印
                List<WaterSensor> start = map.get("start");
                System.out.println(start);

                System.out.println(map.get("n1"));
                System.out.println(map.get("n2"));

                //输出报警信息
                WaterSensor waterSensor = start.get(0);
                return waterSensor.getId() + "连续3条水位线在30以上";
            }
        });

        //TODO 8.输出结果
        selectDS.print(">>>>>>>");

        //TODO 9. 启动任务
        env.execute();

    }
}
