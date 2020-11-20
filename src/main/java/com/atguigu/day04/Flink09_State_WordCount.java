package com.atguigu.day04;

import com.atguigu.day01.Flink01_WordCount_Batch;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhouyanjun
 * @create 2020-11-20 20:52
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        ///3.压平
        SingleOutputStreamOperator<Tuple2<String, Integer>> flatMap = socketTextStream.flatMap(new Flink01_WordCount_Batch.MyFlapMapFunc());
        //4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = flatMap.keyBy(0);

        //5使用状态编程方式实现WordCount功能
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //定义状态  这里是键控状态。这里使用 保存单个的值 的数据类型
            private ValueState<Integer> countState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //这里是初始化状态吧
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("state_count", Integer.class, 0));
            }
            @Override
            //这里是具体的操作数据
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                //首先肯定是从状态中获取数据
                Integer count = countState.value();
                //累加
                count++;
                //更新状态
                countState.update(count);
                //最终返回数据
                return new Tuple2<>(value.f0, count);
            }
        });

        //6.打印
        result.print();

        //7.执行任务
        env.execute();
    }
}
