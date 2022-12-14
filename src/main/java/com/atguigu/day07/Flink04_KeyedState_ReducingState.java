package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author CZQ
 * @Date 2022/7/12 16:31
 * @Version 1.0
 */
public class Flink04_KeyedState_ReducingState {
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

        //5. TODO 针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // TODO 1.定义状态（保存上一次水位值）
            private ReducingState<Integer> reducingState;


            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
        reducingState=getRuntimeContext().getReducingState(new ReducingStateDescriptor<Integer>
                ("reduce-state", new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1+value2;
            }
        }, Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //TODO 3.使用状态
                //1.将当前的水位存入状态做累加  状态中进去的值和出来的值的类型要一致
                reducingState.add(value.getVc());

                //2.从状态中取出计算结果
                Integer sumVc = reducingState.get();
                out.collect(value.getId()+":"+sumVc);


            }
        }).print();
        env.execute();
    }
}
