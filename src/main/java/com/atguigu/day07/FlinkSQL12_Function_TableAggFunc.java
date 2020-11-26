package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author zhouyanjun
 * @create 2020-11-24 19:31
 */
public class FlinkSQL12_Function_TableAggFunc {
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

        //4.注册函数
        tableEnv.registerFunction("Top2Temp", new Top2Temp());

        //5.TableAPI
        Table tableResult = table
                .groupBy("id")
                .flatAggregate("Top2Temp(temp) as (temp,rank)")
                .select("id,temp,rank");

        //6.转换为流打印数据
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");

        //7.执行
        env.execute();
    }

    //需求：来一个数据之后，按照id分组，求前两名的温度。输出的时候，不仅要输出温度信息，还得输出rank值
    //    //TableAggregateFunction<T, ACC> 输出结果集、中间状态。  中间状态是两个温度
    //    //acc中只需要保留2个温度就可以了。所以只用二元组就可以了
    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Tuple2<Double, Double>> {

        //初始化缓冲区。中间状态的初始化
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            return new Tuple2<>(Double.MIN_VALUE, Double.MIN_VALUE);//初始值，设置为double的最小值。反正不能设置0，因为温度有可能是0
        }

        //官网： public void accumulate(ACC accumulator, [user defined inputs]); // MANDATORY
        //计算逻辑
        public void accumulate(Tuple2<Double, Double> buff, Double value) {
            //1 将输入数据跟第一个比较
            if (value > buff.f0) {
                buff.f1 = buff.f0;
                buff.f0 = value;
            } else if (value > buff.f1) {
                //2 将输入数据跟第二个比较
                buff.f1 = value;
            }
        }

        // 官网：public void emitValue(ACC accumulator, Collector<T> out); // OPTIONAL发射数据
        public void emitValue(Tuple2<Double,Double> buffer, Collector<Tuple2<Double,Integer>> collector){
            collector.collect(new Tuple2<>(buffer.f0,1));
            if(buffer.f1 != Double.MIN_VALUE){
                collector.collect(new Tuple2<>(buffer.f1,2));
            }
        }
    }
}

