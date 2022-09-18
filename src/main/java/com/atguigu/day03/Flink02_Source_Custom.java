package com.atguigu.day03;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @Author CZQ
 * @Date 2022/7/6 10:28
 * @Version 1.0
 */
public class Flink02_Source_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO 2.通过定义Source读取数据
        DataStreamSource<WaterSensor> streamSource = env.addSource(new MySource()).setParallelism(2);
        streamSource.print();

        env.execute();
    }
    //通过自定义Source模拟生产WaterSensor数据
 //   public static class MySource implements SourceFunction<WaterSensor>{
        //设置多并行度的方式 可以继承ParallelSourceFunction这个接口
        public static class  MySource implements ParallelSourceFunction<WaterSensor>{
        private Random random = new Random();
        private Boolean isRunning = true;

        @Override
        public void run(SourceContext<WaterSensor> ctx) throws Exception {

            // 随机模拟生成1000个key
            while (true){
            ctx.collect(new WaterSensor("sensor"+random.nextInt(1000),System.currentTimeMillis()
                    ,random.nextInt(100)));
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;

        }
    }
}
