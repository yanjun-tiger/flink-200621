package com.atguigu.day02;

import javafx.beans.property.SimpleStringProperty;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 从kafka读取数据 创建流
 * @author zhouyanjun
 * @create 2020-11-17 12:25
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);//这是为了能够顺序打印

        //2 从kafka读取数据 011指kafka0.11版本及以上
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop102:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //为什么泛型是string？ 因为kafka反序列化之后，消费到的数据是string类型。默认object是因为还可以自定义kafka消费的数据类型
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test",
                new SimpleStringSchema(),
                properties));

        //3.打印
        kafkaDS.print();

        //4.启动任务
        env.execute("Flink03_Source_Kafka");
    }
}
