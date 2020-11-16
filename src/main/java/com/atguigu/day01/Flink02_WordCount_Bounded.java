package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhouyanjun
 * @create 2020-11-16 14:48
 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        //1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 读取文本数据
        DataStreamSource<String> lineDataStream = env.readTextFile("D:\\workspace_idea1\\flink-200621\\input");

        //3 flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDataStream
                = lineDataStream.flatMap(new Flink01_WordCount_Batch.MyFlapMapFunc());
        //4 分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDataStream = wordToOneDataStream.keyBy(0);
        //5 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDataStream.sum(1);
        //6 结果打印
        result.print();
        //7 开启任务
        env.execute("Flink02_WordCount_Bounded");

    }
}
