package com.atguigu.day11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author CZQ
 * @Date 2022/7/15 0:03
 * @Version 1.0
 */
public class Flink10_SQL_Hive {
    public static void main(String[] args) {
        //设置用户权限
        System.setProperty("HADOOP_USER_NAME", "atguigu");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //创建CateLog
        HiveCatalog hiveCatalog = new HiveCatalog("hive", "default", "conf");

        //注册
        tableEnv.registerCatalog("hiveCatalog", hiveCatalog);

        //使用
        tableEnv.useCatalog("hiveCatalog");
        tableEnv.useDatabase("default");
        tableEnv.sqlQuery("select * from student").execute().print();


    }
}
