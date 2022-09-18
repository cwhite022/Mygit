package com.atguigu.day11.function;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author CZQ
 * @Date 2022/7/15 0:04
 * @Version 1.0
 */
public class MyUDF extends ScalarFunction {
    //并不是重新的方法，而是通过反射的方式去调用的，所以名字一定要一样
    public String eval(String word){
        return word.toUpperCase();
    }

}
