package com.atguigu.day03_2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

/**
 * 读取文本数据,将每一行数据拆分成单个单词,对单词进行去重输出。使用redis
 *
 * @author zhouyanjun
 * @create 2020-11-18 20:44
 */
public class Flink03_Distinct_exercise {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2
        DataStreamSource<String> fileDS = env.readTextFile("input");
        //3 flatMap拆分单词
        SingleOutputStreamOperator<String> flatDS = fileDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });

        //4 过滤数据 进行判断 redis  要调用生命周期方法
        SingleOutputStreamOperator<String> filterDS = flatDS.filter(new RichFilterFunction<String>() {
            //因为这里只需要执行一次连接，所以不用建立连接池
            Jedis jedis = null;
            String redisKey = "distinct";

            @Override
            public void open(Configuration parameters) throws Exception {
                //建立连接
                jedis = new Jedis("hadoop102", 6379);
            }

            @Override
            public boolean filter(String value) throws Exception {
                //查询redis中是否有这个数据
                Boolean exist = jedis.sismember(redisKey, value);
                if (!exist) {
                    jedis.sadd(redisKey, value);
                }
                return !exist;//过滤之后的数据集
            }

            @Override
            public void close() throws Exception {
                jedis.close();
            }
        });

        //5 打印
        filterDS.print();

        //6 执行
        env.execute();

    }

}
