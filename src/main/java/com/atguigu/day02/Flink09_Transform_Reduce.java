package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import sun.management.Sensor;

/**
 * @author zhouyanjun
 * @create 2020-11-17 18:32
 */
public class Flink09_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 
        DataStreamSource<String> fileDS = env.readTextFile("sensor");
        //3  把每一条数据转换成javabean
        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] split = s.split(",");
                return new SensorReading(split[0],
                        Long.parseLong(split[1]),
                        Double.parseDouble(split[2]));
            }
        });

        //4 分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");
        //5.计算每个传感器的最高温度以及最近的时间
        SingleOutputStreamOperator<SensorReading> reduce = keyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                return new SensorReading(value1.getId(),
                        value2.getTs(),
                        Math.max(value1.getTemp(), value2.getTemp()));
            }
        });

        //6.打印
        reduce.print();

        //7.执行
        env.execute();
    }
}
