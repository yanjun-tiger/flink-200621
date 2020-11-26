package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.table.descriptors.Json;
/**
 *
 * 读取Kafka数据
 * @author zhouyanjun
 * @create 2020-11-23 18:28
 */
public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        //1创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建表环境============很重要
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2创建Kafka连接器
        tableEnv.connect(new Kafka()
                .topic("test")
                .version("0.11")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG,"testKafkaSource"))
                .withFormat(new Json()) //数据类型
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaInput");

        //3根据表环境======创建表
        Table table = tableEnv.from("kafkaInput");

        //4 table api
        Table tableResult = table.where("id='sensor_1'").select("id,temp");

        //5 SQL
        Table sqlResult = tableEnv.sqlQuery("select id,temp from kafkaInput where id = 'sensor_1'");

        //6 将结果数据打印出来
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult,Row.class).print("sqlResult");

        //7 执行
        env.execute();

    }
}

//{"id":"sensor_1","ts":"1547718205","temp":"35.8"}