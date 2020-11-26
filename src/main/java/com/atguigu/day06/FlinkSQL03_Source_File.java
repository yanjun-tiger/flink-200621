package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.types.Row;

/**
 * 读取文件数据（Csv格式）
 * @author zhouyanjun
 * @create 2020-11-23 15:37
 */
public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2.设置并行度
        env.setParallelism(1);
        //3 创建表的环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4 读取文件数据创建表 这就需要有连接器 要指定属性 配置信息
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        //5 执行SQL查询数据
        Table table = tableEnv.sqlQuery("select id,temp,ts from inputTable where id = 'sensor_1'");

        //6.将表转换为追加流并打印  给输出的时候用的，往外部写，是用追加的模式，
        tableEnv.toAppendStream(table, Row.class).print(); //row.class 是一行数据的类型，要输出，肯定要有类型

        //7. 执行任务
        env.execute();

    }
}
