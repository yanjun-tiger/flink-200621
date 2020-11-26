package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author zhouyanjun
 * @create 2020-11-24 17:58
 */
public class FlinkSQL10_Function_TableFunc {
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

        //3 将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        //4 注册函数
        tableEnv.registerFunction("Split", new Split());

        //5 Table api可以查询
        //因为是表函数，所以得用侧写表。炸裂函数，炸出来其实是一张表的效果。
        //炸裂之后，要和原表字段进行关联
        Table tableResult
                = table.joinLateral("Split(id) as (word,length)").select("id,word,length");

        //6 SQL api 同样可以查询
        //首先要创建表
        tableEnv.createTemporaryView("sensor",sensorDS);
        //T是临时表名，后面跟着字段
        Table sqlResult = tableEnv.sqlQuery("SELECT id, word, length FROM sensor, LATERAL TABLE(split(id)) as T(word, length)");

        //老师是下面的写法
//        Table sqlResult = tableEnv.sqlQuery("select id,word,length from sensor," +
//                "LATERAL TABLE(Split(id)) as T(word, length)");

        //7 转换为流打印数据
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult,Row.class).print("sqlResult");
        //8 执行
        env.execute();
    }

    public static class Split extends TableFunction<Tuple2<String,Integer>> {
        //返回值是void，因为我们可以用collector进行输出，官网就是void
        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collector.collect(new Tuple2<>(s,s.length()));
            }
        }
    }
}
