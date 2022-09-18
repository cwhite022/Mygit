package com.atguigu.day11.function;

import com.atguigu.bean.Acc;
import org.apache.flink.table.functions.AggregateFunction;


/**
 * @Author CZQ
 * @Date 2022/7/15 0:04
 * @Version 1.0
 */
public class MyUDAF extends AggregateFunction<Integer, Acc> {


    @Override
    public Acc createAccumulator() {
        Acc acc = new Acc();
        acc.setAcc(Integer.MIN_VALUE);
        return acc;
    }

    @Override
    public Integer getValue(Acc accumulator) {
        return accumulator.getAcc();
    }






    public void accumulate(Acc acc, Integer iValue) {

        acc.setAcc(Math.max(acc.getAcc(),iValue));

    }
    public void   resetAccumulator(Acc acc) {
         acc.setAcc(Integer.MIN_VALUE);
    }


}
