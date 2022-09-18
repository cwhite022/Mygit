package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
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
public class Flink02_KeyedState_ValueState {
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

        //5. 检测传感器的水位线值，如果连续的两个水位线差值超过10，就输出报警。
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // TODO 1.定义状态（保存上一次水位值）
            private ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
                valueState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",
                        Integer.class));
                //在初始化状态的时候就定义默认值
               /* valueState=getRuntimeContext().getState(new ValueStateDescriptor<Integer>("value-state",
                        Types.INT,0);*/
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //TODO 3.使用状态
                //当状态中没有数据为null的时候给其一个默认值

                Integer lastVc=valueState.value()==null?value.getVc():valueState.value();

                if (Math.abs(value.getVc()-lastVc) >10){
                    out.collect(ctx.getCurrentKey()+"水位超过10，报警！！！");

                }
                //更新状态，将当前的水位保存到状态中
                valueState.update(value.getVc());
            }
        }).print();
        env.execute();
    }
}
