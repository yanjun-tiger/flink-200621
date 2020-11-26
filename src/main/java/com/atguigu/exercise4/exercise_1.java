package com.atguigu.exercise4;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 如果字段名对应不上，那就只有用流来读取数据
 * @author zhouyanjun
 * @create 2020-11-24 8:34
 */
public class exercise_1 {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        //2 从kafka中以流的方式读取数据，而不是以连接器的方式读取数据
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "testKafkaSource");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserialize");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserialize");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("test", new SimpleStringSchema(), properties));

        SingleOutputStreamOperator<String> wordDS = kafkaDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        // 3 将流转换为表  创建临时视图（Temporary View）
        tableEnv.createTemporaryView("wordFromKafka",wordDS,"word");

        //4 sql
        Table sqlResult = tableEnv.sqlQuery("select word,count(word) from wordFromKafka group by word");

        //5 将结果写入Es
        tableEnv.connect(new Elasticsearch()
                .version("6")                      // required: valid connector versions are "6"
                .host("hadoop102", 9200, "http")   // required: one or more Elasticsearch hosts to connect to
                .index("flink_sql_1")                  // required: Elasticsearch index
                .documentType("_doc")
                .bulkFlushMaxActions(1) //根据官网，这里把flush写入外部系统的数据量设定为1
        )
                .inUpsertMode()// Table API中表到DataStream有两种模式，这里是使用追加模式（Append Mode）
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("word", DataTypes.STRING())
                        .field("word_count", DataTypes.BIGINT())).createTemporaryTable("EsPath");

        //输出表 就写入到ES里了
        tableEnv.insertInto("EsPath", sqlResult);

        //6将表转换为追加流进行     打印
        tableEnv.toRetractStream(sqlResult, Row.class).print();

        //7 执行
        env.execute();


    }
}
