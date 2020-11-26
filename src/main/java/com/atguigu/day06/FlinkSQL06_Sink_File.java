package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author zhouyanjun
 * @create 2020-11-23 19:38
 */
public class FlinkSQL06_Sink_File {
    public static void main(String[] args) throws Exception {
        //1 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3 将流转换为表!
        Table table = tableEnv.fromDataStream(sensorDS);

        //4 有了表之后，才可以调用api
        //table api
        Table tableResult = table.groupBy("id").select("id,id.count as ct");

        //5 sql  sql需要有①表执行环境 ②视图
        tableEnv.createTemporaryView("socket", sensorDS);
        Table sqlResult = tableEnv.sqlQuery("select id,temp from socket where id = 'sensor_1'");

        //6 将数据写入文件  连接器写法是类似的
        tableEnv.connect(new FileSystem()
                .path("sensorOut2")).withFormat(new Csv())
                .withSchema(new Schema().field("id", DataTypes.STRING())
                .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("sensorOut2");


        //        tableEnv.from("sensorOut");方式一：输入数据
        //方式二：输出路径
        tableEnv.insertInto("sensorOut2", sqlResult);

        //7.执行任务
        env.execute();
    }
}
