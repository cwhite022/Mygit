package com.atguigu.day04;

import com.atguigu.bean.OrderEvent;
import com.atguigu.bean.TxEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

/**
 * @Author CZQ
 * @Date 2022/7/8 16:58
 * @Version 1.0
 */
public class Flink15 {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.分别获取订单数据和交易数据
        DataStreamSource<String> orderDStream = env.readTextFile("input/OrderLog.csv");
        DataStreamSource<String> txDStream = env.readTextFile("input/ReceiptLog.csv");

        //3.分别将两条流转为javaBean
        SingleOutputStreamOperator<OrderEvent> orderEventDStream = orderDStream.map(new MapFunction<String, OrderEvent>() {
            @Override
            public OrderEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new OrderEvent(
                        Long.parseLong(split[0]),
                        split[1],
                        split[2],
                        Long.parseLong(split[3]));
            }
        });
        SingleOutputStreamOperator<TxEvent> txEventDStream = txDStream.map(new MapFunction<String, TxEvent>() {
            @Override
            public TxEvent map(String value) throws Exception {
                String[] split = value.split(",");
                return new TxEvent(split[0], split[1], Long.parseLong(split[2]));
            }
        });
        //4.将两条流连接起来合成一条流
        ConnectedStreams<OrderEvent, TxEvent> connect = orderEventDStream.connect(txEventDStream);

        //5.将相同交易吗的数据聚合到一块
        ConnectedStreams<OrderEvent, TxEvent> keyBy = connect.keyBy("txId", "txId");

        //6.实时对账
        keyBy.process(new KeyedCoProcessFunction<String, OrderEvent, TxEvent, String>() {
            //缓存订单表的Map集合
            private  HashMap<String, OrderEvent> orderEventHashMap = new HashMap<>();
            //缓存交易表的Map集合
            private  HashMap<String, TxEvent> txEventHashMap = new HashMap<>();
            @Override
            public void processElement1(OrderEvent value, Context ctx, Collector<String> out) throws Exception {
                //1.去对方缓存中查看是否有够关联上的数据
                if (txEventHashMap.containsKey(value.getTxId())){
                    //有能够关联上的数据
                    out.collect("订单:"+value.getOrderId()+"对账成功");
                }else {
                    //对方缓存中没有能够关联上的数据
                    //把自己存到缓存区
                    orderEventHashMap.put(value.getTxId(),value);
                }

            }

            @Override
            public void processElement2(TxEvent value, Context ctx, Collector<String> out) throws Exception {
                //1.去对方缓存中查看是否有够关联上的数据
                if (orderEventHashMap.containsKey(value.getTxId())){
                    //有能够关联上的数据
                    out.collect("订单:"+orderEventHashMap.get(value.getTxId()).getOrderId()+"对账成功");
                }else {
                    //对方缓存中没有能够关联上的数据
                    //把自己存到缓存区
                    txEventHashMap.put(value.getTxId(),value);
                }
            }
        }).print();

        env.execute();

    }
}
