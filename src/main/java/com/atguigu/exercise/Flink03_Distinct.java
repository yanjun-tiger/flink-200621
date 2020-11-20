package com.atguigu.exercise;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.util.JarUtils;
import redis.clients.jedis.Jedis;

/**
 * @author zhouyanjun
 * @create 2020-11-18 11:08
 */
public class Flink03_Distinct {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> fileDS = env.readTextFile("input");

        //3 flatmap
        SingleOutputStreamOperator<String> wordDS = fileDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //4 使用filter算子，因为这个是抽象数据集
        //使用redis/ 富函数，主要是①生命周期②环境上下文
        SingleOutputStreamOperator<String> filter = wordDS.filter(new RichFilterFunction<String>() {
            //声明redis连接 只开启一个jedis连接，所以不用连接池
            Jedis jedis = null; //声明在这里，所以下面的open() filter() close()都可以使用这个变量
            //定义set redisKey
            String redisKey = "distinct";

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new Jedis("hadoop102", 6379);
            }

            @Override
            public boolean filter(String s) throws Exception {
                //查询redis中是否有数据
                Boolean exist = jedis.sismember(redisKey, s);
                if (!exist) {
                    jedis.sadd(redisKey, s);
                }
                return !exist; //过滤掉redis中保存的data
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        });

        //5.打印数据
        filter.print();

        //6.执行任务
        env.execute();
    }
}
