package com.atguigu.day04_2;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

/**
 * 据温度高低将数据发送至不同的流
 * @author zhouyanjun
 * @create 2020-11-20 23:18
 */
public class Flink08_ProcessAPI_KeyedProcessFunc_SideOutPut {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //3 转换成javabean对象。个人认为如果数据的字段很多的话，用样例类可能会好很多
        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0],
                        Long.parseLong(fields[1]),
                        Double.parseDouble(fields[2]));
            }
        });
        //4 keyedby
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        //5 process API才有分流操作
        SingleOutputStreamOperator<SensorReading> highDS = keyedStream.process(new KeyedProcessFunction<Tuple, SensorReading, SensorReading>() {
            @Override
            //处理每一条元素
            public void processElement(SensorReading value, Context ctx, Collector<SensorReading> out) throws Exception {
                //首先，是要获得温度值，这样才可以进行分流操作
                Double temp = value.getTemp();
                //分流操作，可以分多个流；同时每个流的类型可以不一致。
                //开始判断
                if (temp > 30.0D) {
                    out.collect(value);
                } else {
                    ctx.output(new OutputTag<Double>("low"){}, value.getTemp());
                }
            }
        });

        //6 获取侧输出流
        DataStream<Double> lowDS = highDS.getSideOutput(new OutputTag<Double>("low"){});
        //7 打印数据
        highDS.print("high");
        lowDS.print("low");
        //8 执行
        env.execute();



    }
}
