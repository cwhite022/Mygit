package com.atguigu.day04;

import com.atguigu.bean.MarketingUserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * @Author CZQ
 * @Date 2022/7/8 14:06
 * @Version 1.0
 */
public class Flink13_Project_AppAnalysis {
    public static void main(String[] args) throws Exception {
        //1.获取流额执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从自定义source  获取数据
        env.addSource(new AppMarketingDataSource())
                .map(new MapFunction<MarketingUserBehavior, Tuple2<String,Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(MarketingUserBehavior value) throws Exception {
                        return Tuple2.of(value.getBehavior(),1);
                    }
                })
                .keyBy(0)
                //对第二个字段人数进行累加
                .sum(1)
                .print();
        env.execute();
    }
    public static class AppMarketingDataSource extends RichSourceFunction<MarketingUserBehavior> {
        boolean canRun = true;
        Random random = new Random();
        List<String> channels = Arrays.asList("huawwei", "xiaomi", "apple", "baidu", "qq", "oppo", "vivo");
        List<String> behaviors = Arrays.asList("download", "install", "update", "uninstall");

        @Override
        public void run(SourceContext<MarketingUserBehavior> ctx) throws Exception {
            while (canRun) {
                MarketingUserBehavior marketingUserBehavior = new MarketingUserBehavior(
                        (long) random.nextInt(1000000),
                        behaviors.get(random.nextInt(behaviors.size())),
                        channels.get(random.nextInt(channels.size())),
                        System.currentTimeMillis());
                ctx.collect(marketingUserBehavior);
                Thread.sleep(200);
            }
        }

        @Override
        public void cancel() {
            canRun = false;
        }
    }

}
