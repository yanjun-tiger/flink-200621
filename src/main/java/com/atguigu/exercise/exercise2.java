package com.atguigu.exercise;

import com.atguigu.bean.SensorReading;
import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.Properties;

/**
 * @author zhouyanjun
 * @create 2020-11-18 8:59
 */
public class exercise2 {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
//        properties.put(ConsumerConfig.GROUP_ID_CONFIG,) 怎么找到反序列化的全类名的？

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test",
                new SimpleStringSchema(),
                properties));

        //4 应该是要把读取到的每一行数据封装成javabean
        SingleOutputStreamOperator<SensorReading> sensorDS = kafkaDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] words = s.split(",");
                return new SensorReading(words[0],
                        Long.parseLong(words[1]),
                        Double.parseDouble(words[2]));
            }
        });

        //5 再根据温度大小做分流
        SplitStream<SensorReading> split = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading sensorReading) {
                return sensorReading.getTemp() > 30 ? Arrays.asList("high") : Arrays.asList("low");
            }
        });

        //6 选择流
        DataStream<SensorReading> high = split.select("high");
        DataStream<SensorReading> low = split.select("low");

        //7 打印
        high.print("high");
        low.print("low");

        //8执行
        env.execute();
    }
}
