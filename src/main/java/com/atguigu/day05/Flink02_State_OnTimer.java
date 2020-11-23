package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 *需求：如果温度值在10秒内processing time 没有下降，则报警
 * @author zhouyanjun
 * @create 2020-11-21 11:53
 */
public class Flink02_State_OnTimer {
    public static void main(String[] args) throws Exception {
        //1 创造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 从端口读数据
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] split = line.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });
        //3 分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        //4 判断温度5秒内没有下降,则报警 。 要用到状态、要用到时间服务和定时器--->process()
        SingleOutputStreamOperator<String> result = keyedStream.process(new MyKeyedProcessFunc());
        //5 打印输出
        result.print();
        //6 执行
        env.execute();

    }

    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple, SensorReading, String> {
        //①要定义状态
        private ValueState<Double> lastTempState = null;
        //②要再次定义个时间状态
        private ValueState<Long> tsState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            //状态初始化。从状态取值。更新状态
            lastTempState = getRuntimeContext().getState(new ValueStateDescriptor<Double>("last-temp", Double.class));
            tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts",Long.class));
        }

        @Override
        //处理每一条数据。
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //从状态中提取温度值
            Double lastTemp = lastTempState.value();
            //从状态中获取时间戳
            Long lastTs = tsState.value();
            long ts = ctx.timerService().currentProcessingTime() + 5000L; //设置一下触发时间

            //如果是第一条数据,需要注册定时器
            if (lastTs == null) {
                ctx.timerService().registerProcessingTimeTimer(ts);
                tsState.update(ts);
            } else {
                //非第一条数据，则要判断温度是否下降
                if (lastTemp!= null && value.getTemp() < lastTemp) { //和历史数据比较
                    //如果下降温度了。删除之前事件的定时器 从状态中取时间
                    ctx.timerService().deleteProcessingTimeTimer(tsState.value());
                    //同时需要重新注册新的定时器
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                }
            }
            //更新状态
            lastTempState.update(value.getTemp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey() +"连续5秒温度没有下降");
            tsState.clear(); //报警之后，重新设置时间的状态
        }
    }
}
