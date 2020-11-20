package com.atguigu.day04;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2020-11-20 18:12
 */
public class Flink01_Window_EventTime {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2 读取端口数据
        //Flink里，获取时间戳和分配watermark是一步操作。
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] fields = element.split(",");
                return Long.parseLong(fields[1]) * 1000L;
            }
        });
        //3 map
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] words = value.split(",");
                return new Tuple2<>(words[0], 1);
            }
        });


        //4 重分区
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        //5开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDS = keyedDS.timeWindow(Time.seconds(5));

        //6 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowDS.sum(1);

        //7 打印
        result.print();
        //8 执行
        env.execute();

    }
}
