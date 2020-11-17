package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhouyanjun
 * @create 2020-11-17 18:10
 */
public class Flink08_Transform_Max {
    public static void main(String[] args) throws Exception {
        //1 创造环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 读取文件
        DataStreamSource<String> fileDS = env.readTextFile("sensor");
        //3 将每一行数据转换为JavaBean why?
        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });
        //4 按照id分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");
        //5 求每个传感器中最高温度
        SingleOutputStreamOperator<SensorReading> maxResult = keyedStream.max("temp");
        SingleOutputStreamOperator<SensorReading> maxByResult = keyedStream.maxBy("temp");

        //6 打印结果
        maxResult.print("max");
        maxByResult.print("maxBy");
        //7 执行
        env.execute();

    }
}
