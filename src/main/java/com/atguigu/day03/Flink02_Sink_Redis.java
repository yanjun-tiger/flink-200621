package com.atguigu.day03;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.io.StdIn;

/**
 * @author zhouyanjun
 * @create 2020-11-18 13:12
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从文件中读取数据创建流
        DataStreamSource<String> inputDS = env.readTextFile("sensor");

        //3.将数据写入Redis
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(6379)
                .build();

        inputDS.addSink(new RedisSink<String>(config, new MyRedisMapper()));

        //4.执行任务
        env.execute();

    }

    public static class MyRedisMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            //redis hash结构，key field value。这里设置key是sensor
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        @Override
        public String getKeyFromData(String data) {
            String[] fields = data.split(",");
            //id
            return fields[0];
        }

        @Override
        public String getValueFromData(String data) {
            String[] fields = data.split(",");
            //温度
            return fields[2];
        }
    }
}
