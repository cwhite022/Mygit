package com.atguigu.day08;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author CZQ
 * @Date 2022/7/13 16:01
 * @Version 1.0
 */
public class FlinkTest01 {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        //2.从端口读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 9999);

        //3. 修改其并行度为2 因为socketTextStream是一个不能够并行读取的source
        SingleOutputStreamOperator<String> mapDS =
                socketTextStream.map(line -> line).setParallelism(2);
        //4.重分区
        mapDS.broadcast().print("broadcast>>>").setParallelism(4);
        mapDS.global().print("global>>>").setParallelism(4);
        //Forward partitioning does not allow change of parallelism 不允许修改并行度
        /*mapDS.forward().print("forward>>>").setParallelism(4);*/

        //5.启动任务
        env.execute();

    }
}
