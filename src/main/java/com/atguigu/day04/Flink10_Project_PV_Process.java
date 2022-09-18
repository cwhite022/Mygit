package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author CZQ
 * @Date 2022/7/8 11:05
 * @Version 1.0
 */
public class Flink10_Project_PV_Process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //这里如果把并行度设置为2的话，结果会改变，因为使用process的时候，并行度改为2的话，keyBy的时候会默认为轮训的方式发送的，这样便会在两个并行度（算子实例）
        // 上进行keyby，各自计算各自分区结果里的数据
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                .process(new ProcessFunction<String, Tuple2<String,Integer>>() {

                    private Integer count = 0;
                    @Override
                    public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                        //1.将数据按照逗号切分
                        String[] split = value.split(",");

                        //2.将数据组成JavaBean
                        UserBehavior userBehavior = new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4])
                        );

                        //3.过滤出PV的数据
                        if ("pv".equals(userBehavior.getBehavior())){
                            count++;
                            out.collect(Tuple2.of("pv",count));
                        }
                    }
                }).print();
        env.execute();

    }
}
