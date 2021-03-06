package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhouyanjun
 * @create 2020-11-17 16:36
 */
public class Flink05_Transform_Map {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 从文件读取数据
        DataStreamSource<String> fileDS = env.readTextFile("sensor");

        //3 将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = fileDS.map(new MapFunction<String, SensorReading>() {
            @Override
            //value就是输入的每行文本数据
            public SensorReading map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new SensorReading(fileds[0],
                        Long.parseLong(fileds[1]),
                        Double.parseDouble(fileds[2])
                );
            }
        });
        //4 打印
        sensorDS.print();
        //5 执行
        env.execute("Flink05_Transform_Map");
    }
}
