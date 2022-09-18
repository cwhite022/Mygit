package com.atguigu.day07;

import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

/**
 * @Author CZQ
 * @Date 2022/7/13 11:02
 * @Version 1.0
 */
public class Flink09_STateBackend {
    public static void main(String[] args) throws IOException {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TODO 内存级别
        //老版本1.1.3（包括）之前的版本
        env.setStateBackend(new MemoryStateBackend());

        //新版本写法

        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());

        // TODO 文件级别
        //老版本写法
        env.setStateBackend(new FsStateBackend("hdfs://hadoop102:8020/ck"));

        //新版本写法
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //或者下面这种 下面这种方式的优势可以设置其他参数，比如缓冲区大小
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));

        // TODO RocksDB
        //老版本
//        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        //建议使用这种方式
        env.setStateBackend(new RocksDBStateBackend("hdfs://hadoop102:8020/ck"));

        //新版本
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");

// 下面这个方法可以设置更多其他参数
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));

        //不对齐的Barrier
        env.getCheckpointConfig().enableUnalignedCheckpoints();

    }
}
