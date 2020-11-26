package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.expressions.E;
import org.apache.flink.types.Row;

/**
 *将数据写入ES
 * @author zhouyanjun
 * @create 2020-11-23 20:11
 */
public class FlinkSQL08_Sink_ES_Append {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        //4.TableAPI
        Table tableResult = table.select("id,temp").where("id='sensor_1'");

        //5.SQL
        tableEnv.createTemporaryView("socket", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from socket where id='sensor_1'");

        //6.将数据写入ES
        tableEnv.connect(new Elasticsearch()
                .version("6")                      // required: valid connector versions are "6"
                .host("hadoop102", 9200, "http")   // required: one or more Elasticsearch hosts to connect to
                .index("flink_sql")                  // required: Elasticsearch index
                .documentType("_doc")
                .bulkFlushMaxActions(1) //根据官网，这里把flush写入外部系统的数据量设定为1
        )
                .inAppendMode() // Table API中表到DataStream有两种模式，这里是使用追加模式（Append Mode）
                .withFormat(new Json())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE())).createTemporaryTable("EsPath");

             tableEnv.insertInto("EsPath", tableResult);

        //将表转换为追加流进行打印
        tableEnv.toAppendStream(tableResult, Row.class).print();

        //7.执行任务
        env.execute();
    }

}
