package com.atguigu.day04;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.xcontent.XContentType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @Author CZQ
 * @Date 2022/7/7 11:50
 * @Version 1.0
 */
public class Flink06_Sink_Custom {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 9999);

        //3.先将数据转为JavaBean（Warnsenor）
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {

            @Override
            public WaterSensor map(String value) throws Exception {
                String[] split = value.split(",");
                WaterSensor waterSensor = new WaterSensor(split[0], Long.parseLong(split[1]),
                        Integer.parseInt(split[2]));
                return waterSensor ;
            }
        });
        //TODO 4.自定义sink将数据写入MYSQL
        map.addSink(new MySink());


        env.execute();
    }
  //  public static class MySink implements SinkFunction<WaterSensor>{
    public static class MySink extends RichSinkFunction<WaterSensor> {
        private Connection connection;
        private PreparedStatement pstm;

      @Override
      public void close() throws Exception {
          //关闭资源
          pstm.close();
          connection.close();
      }

      @Override
      public void open(Configuration parameters) throws Exception {
          //1.创建连接
           connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?useSSL=false", "root",
                  "123456");

          //2.创建语句预执行者
           pstm = connection.prepareStatement("insert into sensor values (?,?,?)");

      }

      @Override
        public void invoke(WaterSensor value, Context context) throws Exception {

            //3.给占位符赋值
            pstm.setString(1, value.getId());
            pstm.setLong(2, value.getTs());
            pstm.setInt(3, value.getVc());

            //执行语句
            pstm.execute();


        }
    }
}
