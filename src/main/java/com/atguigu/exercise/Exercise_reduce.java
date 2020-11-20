package com.atguigu.exercise;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2020-11-18 8:39
 */
public class Exercise_reduce {
    public static void main(String[] args) throws Exception {
        //1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 读取文件
        DataStreamSource<String> fileDS = env.readTextFile("input");

        //3 flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = fileDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //4
        KeyedStream<Tuple2<String, Integer>, Tuple> tuple2TupleKeyedStream = tupleDS.keyBy(0);
        //5
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceDS = tuple2TupleKeyedStream.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return new Tuple2<>(t2.f0, t1.f1 + t2.f1);
            }
        });

        //6
        reduceDS.print();
        //7
        env.execute();
    }
}
