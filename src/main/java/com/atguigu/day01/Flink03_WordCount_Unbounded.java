package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhouyanjun
 * @create 2020-11-16 18:04
 */
public class Flink03_WordCount_Unbounded {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 从端口获取数据创建数据流
        DataStreamSource<String> lineDS = env.socketTextStream("hadoop102", 7777);
        //3 flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS
                = lineDS.flatMap(new Flink01_WordCount_Batch.MyFlapMapFunc());
        //4 分组 groupby
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);
        //5 聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);
        //6 执行
        result.print();
        //7 启动任务
        env.execute();

    }
}
