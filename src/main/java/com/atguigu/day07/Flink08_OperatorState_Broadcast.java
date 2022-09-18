package com.atguigu.day07;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/7/13 10:29
 * @Version 1.0
 */
public class Flink08_OperatorState_Broadcast {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.获取两条流
        DataStreamSource<String> localhostStream = env.socketTextStream("localhost", 9999);

        DataStreamSource<String> hadoopStream = env.socketTextStream("hadoop102", 9999);

        //3.定义一个状态并广播
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<>("map", String.class, String.class);

        //4.广播状态
        BroadcastStream<String> broadcastStream = localhostStream.broadcast(mapStateDescriptor);

        //5.合并两条流
        BroadcastConnectedStream<String, String> connect = hadoopStream.connect(broadcastStream);

        //6.对合并后的流做处理，通过localhost中的数据控制hadoop102中的逻辑
        connect.process(new BroadcastProcessFunction<String, String, String>() {
            /**
             * 处理普通流中的数据（hadoop102）
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                String aSwitch = broadcastState.get("switch");
                if ("1".equals(aSwitch)){
                    out.collect("执行逻辑1...");
                }else if ("2".equals(aSwitch)){
                    out.collect("执行逻辑2....");
                }else {
                    out.collect("执行逻辑3...");
                }
            }

            /**
             * 处理广播流中的数据（localhost）
             * @param value
             * @param ctx
             * @param out
             * @throws Exception
             */

            @Override
            public void processBroadcastElement(String value, Context ctx, Collector<String> out) throws Exception {

                //1.提取广播状态
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                //2.将数据保存至广播状态中
                broadcastState.put("switch",value);
            }
        }).print();
        env.execute();

    }
}
