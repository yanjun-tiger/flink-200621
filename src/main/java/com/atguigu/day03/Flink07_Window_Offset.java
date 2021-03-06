package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2020-11-18 19:56
 */
public class Flink07_Window_Offset {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.读取端口数据
        //DataStreamSource<String> input = env.readTextFile("input");
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        //3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> tupleDS = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));//泛型<>里是灰色的，就可以不用写
                }
            }
        });

        //4.重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = tupleDS.keyBy(0);

        //5.开窗
        //怎么查看of的源码的？
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> window
                = keyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)));

        //6 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);
        //7 打印
        sum.print();
        //7 执行
        env.execute();
    }
}
