package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
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
public class Flink06_KeyedState_MapState {
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

        //5. TODO 去重：去掉重复的水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // TODO 1.定义状态
            private MapState<Integer,WaterSensor> mapState;





            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
                /*mapState=getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-State",
                        Integer.class, WaterSensor.class));*/
                mapState=getRuntimeContext().getMapState(new MapStateDescriptor<Integer, WaterSensor>("map-State",
                        Types.INT,Types.POJO(WaterSensor.class)));



            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //TODO 3.使用状态
                //1.判断key（Vc）是否存在
                if (!mapState.contains(value.getVc())){
                    //当前key不在状态中
                    mapState.put(value.getVc(),value);
                    //vc不存在则输出，存在则不输出
                out.collect(value.toString());
                }

//                out.collect(mapState.get(value.getVc()).toString());

            }
        }).print();
        env.execute();
    }
}
