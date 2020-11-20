package com.atguigu.day04;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 这个有问题。我需要再来一次。
 * @author zhouyanjun
 * @create 2020-11-20 19:41
 */
public class Flink06_ProcessAPI_KeyedProcessFunc {
    public static void main(String[] args) {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.将每一行数据转换为JavaBean。这里为什么要转化为样例类？
        //map是映射的意思
        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //4 分组
        KeyedStream<SensorReading, Tuple> keyedDS = sensorDS.keyBy("id");

        //5.使用ProcessAPI处理数据 -----> 不使用reduce来进行wordcount操作
        keyedDS.process(new MyKeyedProcessFunc());

    }
    //输出类型，为什么是string？
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple,SensorReading,String> {
        //Context ctx   可以获取时间；能够通过时间使用定时器和侧输出流。context是普通富函数没有的。
        //能做的事更多了，如下显示

        //生命周期方法
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }
        //生命周期方法
        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //状态编程相关  为什么没有使用对象调用方法？
            RuntimeContext runtimeContext= getRuntimeContext();

            //获取当前的Key以及时间戳
            Tuple currentKey = ctx.getCurrentKey();
            Long timestamp = ctx.timestamp();

            //定时服务相关
            TimerService timerService = ctx.timerService();
            timerService.currentProcessingTime();
            timerService.registerProcessingTimeTimer(111L);//定闹钟
            timerService.deleteProcessingTimeTimer(111L);//删除闹钟

        }

        //定时服务相关
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            super.onTimer(timestamp, ctx, out);
        }
    }
}
