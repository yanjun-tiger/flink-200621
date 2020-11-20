package com.atguigu.day04_2;

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
 * @create 2020-11-20 21:28
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {
         //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //3 flatmap
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new Flink01_WordCount_Batch.MyFlapMapFunc());
        //4 keyby
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        //5 使用状态编程方式实现WordCount功能。需要使用富函数，生命周期、状态编程
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            //声明状态 状态还有自己的数据结构
            private ValueState<Integer> countState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                //生命周期方法，省略了this.
                countState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count_state", Integer.class, 0));
            }

            @Override
            //状态，就体现了keyby的重要性
            public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
                //获取状态中的数据
                Integer count = countState.value();
                //业务操作，累加
                count++;
                //更新状态
                countState.update(count);
                //最终返回数据
                return new Tuple2<>(value.f0, count);
            }
        });
        //打印
        result.print();
        //执行
        env.execute();

    }
}
