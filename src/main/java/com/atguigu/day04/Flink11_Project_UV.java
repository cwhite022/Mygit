package com.atguigu.day04;

import com.atguigu.bean.UserBehavior;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * @Author CZQ
 * @Date 2022/7/8 11:42
 * @Version 1.0
 */
public class Flink11_Project_UV {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从文件读数据
        env
                .readTextFile("input/UserBehavior.csv")
                .flatMap(new FlatMapFunction<String, Tuple2<String,Long>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] split = value.split(",");
                        UserBehavior userBehavior = new UserBehavior(
                                Long.parseLong(split[0]),
                                Long.parseLong(split[1]),
                                Integer.parseInt(split[2]),
                                split[3],
                                Long.parseLong(split[4])
                        );

                        //过滤出PV的数据
                        if ("pv".equals(userBehavior.getBehavior())){
                            out.collect(Tuple2.of("uv",userBehavior.getUserId()));
                        }

                    }

                })
                .keyBy(0)
                .process(new KeyedProcessFunction<Tuple, Tuple2<String, Long>, Tuple2<String,Integer>>() {

                   private   HashSet<Object> uids = new HashSet<>();
                    @Override
                    public void processElement(Tuple2<String, Long> value, Context ctx, Collector<Tuple2<String,
                            Integer>> out) throws Exception {
                        //1.将用户id存入Set集合
                        uids.add(value.f1);

                        //2.取出set集合元素个数
                        int uidCount = uids.size();
                        //3.组成Tuple2元组返回
                        out.collect(Tuple2.of("uv",uidCount));
                    }
                }).print();
        env.execute();
}
}
