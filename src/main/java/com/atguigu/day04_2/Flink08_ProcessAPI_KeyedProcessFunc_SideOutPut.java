package com.atguigu.day04_2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhouyanjun
 * @create 2020-11-20 23:18
 */
public class Flink08_ProcessAPI_KeyedProcessFunc_SideOutPut {
    public static void main(String[] args) {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);
        //3 flatmap
        //4 keyedby
        //5

    }
}
