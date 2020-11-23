package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 需求：检测传感器的温度值，如果连续的两个温度差值超过10度，就输出报警。
 * @author zhouyanjun
 * @create 2020-11-21 11:31
 */
public class Flink01_State_Temp {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 读取端口数据创建流
        SingleOutputStreamOperator<SensorReading> sensorDataStream = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });
        //3分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDataStream.keyBy("id");

        //4 判断温度是否跳变,如果跳变超过10度,则报警  使用富函数---使用状态
        //要提取温度 id 上次温度 这次温度
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> result = keyedStream.flatMap(new RichFlatMapFunction<SensorReading, Tuple3<String, Double, Double>>() {
            //这是一种存放状态的数据结构
            private ValueState<Double> lastTempState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //给状态做初始化
                lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            }

            @Override
            public void flatMap(SensorReading value, Collector<Tuple3<String, Double, Double>> out) throws Exception {
                //给状态赋值，从状态取数据，更新状态
                Double lastTemp = lastTempState.value();
                //获取当前温度值
                Double curentTemp = value.getTemp();

                if (lastTemp != null && Math.abs(lastTemp - curentTemp) > 10.0) {
                    out.collect(new Tuple3<>(value.getId(), curentTemp, lastTemp));
                }

                //别忘了更新状态
                lastTempState.update(curentTemp);

            }
        });

        //5 打印输出
        result.print();
        //6 执行
        env.execute();
    }
}
