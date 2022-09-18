package com.atguigu.day11.function;

import com.atguigu.bean.Top2;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.util.Collector;


/**
 * @Author CZQ
 * @Date 2022/7/15 0:04
 * @Version 1.0
 */
public class MyUDTAF extends TableAggregateFunction<Tuple2<Double,Integer>, Top2> {
    @Override
    public Top2 createAccumulator() {
        Top2 top2 = new Top2();
        top2.setFirst(Double.MIN_VALUE);
        top2.setSecond(Double.MIN_VALUE);
        return top2;
    }

    public void accumulate(Top2 acc, Double value) {
        if (value > acc.getFirst()) {
            acc.setSecond(acc.getFirst());
            acc.setFirst(value);
        } else if (value > acc.getSecond()) {
            acc.setSecond(value);
        }
    }
    public void emitValue(Top2 acc, Collector<Tuple2<Double, Integer>> out) {
        Double first = acc.getFirst();
        Double second = acc.getSecond();
        if (first>Double.MIN_VALUE){
            out.collect(Tuple2.of(first,1));
        }
        if (second>Double.MIN_VALUE){
            out.collect(Tuple2.of(second ,2));
        }

    }

}
