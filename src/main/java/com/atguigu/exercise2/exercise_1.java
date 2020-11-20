package com.atguigu.exercise2;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * 读取Kafka test1主题的数据计算WordCount存入MySQL.
 * @author zhouyanjun
 * @create 2020-11-20 8:38
 */

public class exercise_1 {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 读取kafka数据创建source 流
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"test1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test1",
                new SimpleStringSchema(),//序列化
                properties));
        //3
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        });
        //4
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);
        //5
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedDS.sum(1);
        //存入MySQL 新建sink
        sum.addSink(new jdbcSink());

        //执行
        env.execute();
    }


    public static class jdbcSink extends RichSinkFunction<Tuple2<String, Integer>> {
            Connection connection = null;
            PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            preparedStatement = connection.prepareStatement("insert into wordcount(word,word_count) values(?,?) on duplicate key update word=? ");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            String word = value.f0;
            Integer word_count = value.f1;

            preparedStatement.setString(1,word);
            preparedStatement.setInt(2,word_count);
            preparedStatement.setString(3,word);

            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }
    }
}
//mysql 建表语句

//CREATE TABLE wordcount(
//        word VARCHAR(255),
//    word_count INT,
//    PRIMARY KEY(word)
//);

