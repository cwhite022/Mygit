package com.atguigu.day07;

import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author CZQ
 * @Date 2022/7/12 16:31
 * @Version 1.0
 */
public class Flink03_KeyedState_ListState {
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

        //5. TODO 针对每个传感器输出最高的3个水位值
        keyedStream.process(new KeyedProcessFunction<String, WaterSensor, String>() {
            // TODO 1.定义状态（保存上一次水位值）
            private ListState<Integer> listState;


            @Override
            public void open(Configuration parameters) throws Exception {
                //TODO 2.初始化状态
                listState=getRuntimeContext().getListState(new ListStateDescriptor<Integer>("list-State",
                        Integer.class));

            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<String> out) throws Exception {

                //TODO 3.使用状态

                //1.将当前水位保存到状态中
                listState.add(value.getVc());

                //2.取出状态中的值
                Iterable<Integer> iterable = listState.get();

                //3.创建一个List集合用来存放迭代器中的数据
                ArrayList<Integer> listVc = new ArrayList<>();
                for (Integer integer : iterable) {
                    listVc.add(integer);
                }

                //4.判断集合中的数据是否大于三个，如果大于三个则排序，取前三个
                if (listVc.size()>3){
                    listVc.sort(new Comparator<Integer>() {
                        @Override
                        public int compare(Integer o1, Integer o2) {
                            return o2-o1;
                        }
                    });
                //5.删除经过排序后的最后一个元素（最小的数据），list集合中最多有4个元素，删除排序后的第四个
                listVc.remove(3);
                }

                //6.更新状态 addAll追加 Updae则是覆盖
                listState.update(listVc);
                out.collect(listVc.toString());


            }
        }).print();
        env.execute();
    }
}
