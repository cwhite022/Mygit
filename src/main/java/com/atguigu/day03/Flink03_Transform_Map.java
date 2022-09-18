package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/6 10:58
 * @Version 1.0
 */
public class Flink03_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
//        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);
        // 读文件并不一定是有界数据，也有可能是无界的
        DataStreamSource<String> streamSource = env.readTextFile("input/sensor.txt");
        //TODO 3. 使用Map将从端口读出来的字符串转为WaterSensor(JAVABean)
   /*     SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
            }
        });
        map.print();*/
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MyMap());
        map.print();
        env.execute();
    }
    // TODO 富函数
    public static class MyMap extends RichMapFunction<String,WaterSensor> {
        /** 生命周期方法都是每个并行度调用一次，当从文件读数据时，每个并行度调用两次
         * 生命周期方法 Close最后被调用，预示着程序的结束,
         * 适用场景：做一些收尾工作，比如资源的释放，关闭连接
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            System.out.println("close...");
        }
        /**
         * 生命周期方法,Open最先被调用，预示着程序的开始，每个并行度调用一次，当从文件读数据时，每个并行度调用一次
         * 适用场景：做一些初始化操作，比如创建连接
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open...");
        }

        @Override
        public WaterSensor map(String value) throws Exception {
            //getRuntimeContext方法的作用，可以获取到更多信息
            System.out.println(getRuntimeContext().getJobId());
            System.out.println(getRuntimeContext().getTaskName());
            String[] split = value.split(",");
            return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
        }
    }
}
