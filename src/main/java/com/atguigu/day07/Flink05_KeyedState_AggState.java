package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/7/12 16:31
 * @Version 1.0
 */
public class Flink05_KeyedState_AggState {
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

        //5. TODO 计算每个传感器的平均水位
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // TODO 1.定义状态
            private AggregatingState<Integer,Double> aggregatingState;





            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
                aggregatingState=getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer,
                        Tuple2<Integer,Integer>, Double>("agg-State",
                        new AggregateFunction<Integer, Tuple2<Integer,Integer>, Double>() {
                    @Override
                    public Tuple2<Integer, Integer> createAccumulator() {
                        return Tuple2.of(0,0);
                    }

                    @Override
                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                        return Tuple2.of(accumulator.f0+value,accumulator.f1+1);
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                        return accumulator.f0*1D/accumulator.f1;
                    }

                    @Override
                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                        return Tuple2.of(a.f0+b.f0,a.f1+b.f1);
                    }
                }, Types.TUPLE(Types.INT,Types.INT)));


            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //TODO 3.使用状态
                //1.将数据保存到状态中
                aggregatingState.add(value.getVc());

                //2.取出状态中的计算结果
                Double avgVc = aggregatingState.get();
                out.collect(value.getId()+":"+avgVc);


            }
        }).print();
        env.execute();
    }
}
