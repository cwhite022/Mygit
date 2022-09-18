package com.atguigu.day11.function;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @Author CZQ
 * @Date 2022/7/15 0:05
 * @Version 1.0
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public class MyUDTF extends TableFunction<Row> {

    public void eval(String str) {
        String[] words = str.split(" ");
        for (String word : words) {
            collect(Row.of(word,word.length()));

        }

    /*    for (String s : str.split(" ")) {

            collect(Row.of(s, s.length()));
        }*/
    }


    }

