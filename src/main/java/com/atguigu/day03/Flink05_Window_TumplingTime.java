package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2020-11-18 19:21
 */
//滚动窗口函数
public class Flink05_Window_TumplingTime {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2读取端口数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //3 压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));//泛型<>里是灰色的，就可以不用写
                }
            }
        });
        //4 重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = tupleDS.keyBy(0);

        //5简化版本滚动时间开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDStream = keyedDS.timeWindow(Time.seconds(5));
        //6 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = windowDStream.sum(1);
        //打印
        sum.print();
        //7执行
        env.execute();
    }
}
