package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author zhouyanjun
 * @create 2020-11-17 19:25
 */
public class Flink12_Transform_Union {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> fileDS = env.readTextFile("sensor");
        //3 把每条数据转换成javaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        //4 根据温度大小做分流
        SplitStream<SensorReading> split = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Arrays.asList("high") : Arrays.asList("low");
            }
        });
        //5 选择流
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");
        //6 求并集
        DataStream<SensorReading> all = high.union(low);
        //7 打印数据
        all.print();
        //8 执行
        env.execute();

    }

}
