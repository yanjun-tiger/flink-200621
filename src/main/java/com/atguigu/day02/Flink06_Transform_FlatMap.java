package com.atguigu.day02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2020-11-17 16:47
 */
public class Flink06_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 从文件中读取 数据流  ----要记得，flink里面都是数据流
        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        //3 flatmap操作
        SingleOutputStreamOperator<String> flatMapDS = fileDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });
        //4 打印
        flatMapDS.print();
        //5 执行
        env.execute("Flink06_Transform_FlatMap");
    }
}
