package com.atguigu.exercise;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.HashSet;

/**
 * @author zhouyanjun
 * @create 2020-11-18 9:26
 */
public class exercise3 {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2
        DataStreamSource<String> fileDS = env.readTextFile("sensor");


        //3 flatmap
        SingleOutputStreamOperator<String> wordDS = fileDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        //把数据写入redis
        //a 获取连接
        //b 将获得的数据写入redis

        //c 完成数据库操作之后，归还连接




        //
        wordDS.print();

        env.execute();


    }
}
