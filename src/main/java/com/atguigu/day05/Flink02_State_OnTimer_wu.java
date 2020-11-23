package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import com.sun.corba.se.spi.servicecontext.SendingContextServiceContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.awt.*;

/**
 * 如果温度值在10秒钟之内(processing time)连续上升，则报警
 *
 * @author zhouyanjun
 * @create 2020-11-22 20:39
 */
public class Flink02_State_OnTimer_wu {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //3 把接受的数据封装成Javabean对象
        SingleOutputStreamOperator<SensorReading> sensorDS = input.map(new MapFunction<String, SensorReading>() {

            @Override
            public SensorReading map(String value) throws Exception {
                String[] files = value.split(",");
                return new SensorReading(files[0], Long.parseLong(files[1]), Double.parseDouble(files[2]));
            }
        });
        //4 keyby
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");
        //5 计算逻辑，开窗不适用，用定时器会适用
        SingleOutputStreamOperator<String> result = keyedStream.process(new TempIncreaseWarning());//别忘了，写完了类，记得把后续的代码写完
        //6 打印
        result.print();
        //7 执行
        env.execute();

    }

    public static class TempIncreaseWarning extends KeyedProcessFunction<Tuple, SensorReading, String> {
        //需要用到状态，所以需要生命周期方法
        //初始化状态。要不断与前一个温度值进行比较。
        private ValueState<Double> lastTempState = null;
        //根据当前时间设置定时器。
        private ValueState<Long> curTimerTsState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //根据运行时上下文，初始化状态。
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-Temp", Double.class,0D)); //默认状态的值是null
            curTimerTsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("current-timer-ts", Long.class,0L));

        }

        @Override
        //处理每一条数据
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //获取状态的值
            Double lastTemp = lastTempState.value();
            Long curTimer = curTimerTsState.value();

            Double curTemp = value.getTemp();

            //如果没有定时器，注册个定时器
            if (curTimer == 0 && curTemp >lastTemp) { // if(lastTemp == null && curTimer ==null){ 温度的状态仍然需要保留，为了与后续的温度进行比较
                long ts = ctx.timerService().currentProcessingTime() + 5000L;//设置触发时间
                //注册个定时器
                ctx.timerService().registerProcessingTimeTimer(ts);
                //更新下时间，把所处理的这个数据的时间保存下来。保存到状态里
                curTimerTsState.update(ts);
            } else if (curTemp < lastTemp) { //温度比上次低，就不用报警。就是删除 所处理的那条数据的定时器
                ctx.timerService().deleteProcessingTimeTimer(curTimerTsState.value());
                //为了开始下一轮的注册定时器。需要将时间状态恢复到默认情况
                curTimerTsState.clear(); //温度的状态仍然需要保留，为了进行比较
            }

            lastTempState.update(curTemp);
        }

        //触发定时器的方法
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() + "连续5秒温度没有下降，所以报警！");
            //清空之后，可以继续注册新一轮数据的定时器
            curTimerTsState.clear();
        }
    }
}
