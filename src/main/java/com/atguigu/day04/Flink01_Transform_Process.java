package com.atguigu.day04;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.util.HashMap;

/**
 * @Author CZQ
 * @Date 2022/7/7 0:38
 * @Version 1.0
 */
public class Flink01_Transform_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.使用process从端口读取到的数据转为WaterSensor(Map)
        SingleOutputStreamOperator<WaterSensor> map = streamSource.process(new ProcessFunction<String,
                WaterSensor>() {
            @Override
            public void processElement(String value, Context ctx, Collector<WaterSensor> out) throws Exception {
                String[] split = value.split(",");
                out.collect(new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2])));
            }
        });
        //4.使用keyby将相同的id
        KeyedStream<WaterSensor, Tuple> keyBy = map.keyBy("id");

        //process对应你的操作通常在Flink没有现有实现好的功能的时候才使用
        //TODO 5.使用Process实现Sum的功能对VC求和
        SingleOutputStreamOperator<WaterSensor> process = keyBy.process(new KeyedProcessFunction<Tuple, WaterSensor,
                WaterSensor>() {

            //定义一个累加器，用来保存上一次累加后的结果
           // private Integer lastSumVc = 0; 这里无法做到区分id
            private  HashMap<String, Integer> lastSumMap = new HashMap<>();

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //首先判断这条数据是否是当前id第一条数据（依据map中是否有这个id）
                if (lastSumMap.containsKey(value.getId())){

                //1.根据传进来数据的id从Map中获取到对应id的累加值
                Integer lastSumVc = lastSumMap.get(value.getId());
                //2.累加vc值
                lastSumVc += value.getVc();
                //3.把累加后的结果更新到Map中
                lastSumMap.put(value.getId(),lastSumVc);
                out.collect(new WaterSensor(value.getId(),value.getTs(),lastSumVc));
                }else {
                    //当前这个id不在map中，则证明是第一条数据，直接将vc值存入map即可
                    lastSumMap.put(value.getId(),value.getVc());
                    out.collect(value);
                }
            }
        });
        process.print();

       // keyBy.sum("vc").print();
        env.execute();
    }
}
