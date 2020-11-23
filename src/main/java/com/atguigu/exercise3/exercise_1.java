package com.atguigu.exercise3;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 3.使用事件时间处理数据,编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,
 * Watermark设置延迟2秒钟,允许迟到数据2秒钟,再迟到的数据放置侧输出流。
 *
 * @author zhouyanjun
 * @create 2020-11-21 8:36
 */
public class exercise_1 {
    public static void main(String[] args) throws Exception {
        //1 创造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 端口读取数据
        SingleOutputStreamOperator<String> socketDS = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    //提取时间戳
                    public long extractTimestamp(String element) {
                        String[] fileds = element.split(",");
                        return Long.parseLong(fileds[1]) * 1000;
                    }
                });

        //3 map  每个传感器发送温度的次数, 就是对id计数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return new Tuple2<>(fields[0], 1);
            }
        });


        //4 用keyby
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordToOneDS.keyBy("0");

        //5 应该是开窗
        WindowedStream<Tuple2<String, Integer>, Tuple, TimeWindow> windowDS
                = keyedStream.timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("late_data") {
                });

        //6 计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowDS.sum(1);

        //7 打印
        result.print("main");
        //8 获取侧输出流
        DataStream<Tuple2<String, Integer>> sideOutput = result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("late_data") {
        });
        sideOutput.print("sideOutPut");
        //9 执行任务
        env.execute();
    }
}
