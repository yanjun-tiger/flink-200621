package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author zhouyanjun
 * @create 2020-11-20 18:43
 */
public class Flink02_Window_EventTime_Late {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 时间语义：使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //2 读取端口数据 读取事件时间
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
            @Override
            public long extractAscendingTimestamp(String element) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //3 压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });

        //4 重分区 只有重分区，使用窗口才有意义
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        //5 开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowedStream = keyedDS
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))  //好像有点不清楚
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("sideOutPut"){});//内部类，加{}。看注解

        //6.计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //7.打印
        result.print("main");

        //获取侧输出流数据
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("sideOutPut") {
        });
        sideOutput.print("sideOutPut");

        //8.执行任务
        env.execute();

    }
}
