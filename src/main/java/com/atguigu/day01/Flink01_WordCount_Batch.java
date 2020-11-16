package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
//这个依赖倒错了，就容易报错

/**
 * @author zhouyanjun
 * @create 2020-11-16 14:47
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        //1 创建Flink程序的入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2 读取文件数据 "hello atguigu" 一行一行的读取
        DataSource<String> lineDS = env.readTextFile("D:\\workspace_idea1\\flink-200621\\input");

        //3 压平操作 "hello atguigu" => (hello 1),(atguigu 1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDS
                = lineDS.flatMap(new MyFlapMapFunc());

        //4 分组 批处理的数据集都是dataSet
        UnsortedGrouping<Tuple2<String, Integer>> groupByDS = wordToOneDS.groupBy(0);
        //5 聚合计算
        AggregateOperator<Tuple2<String, Integer>> result = groupByDS.sum(1);
        //6 打印结果
        result.print();

    }

    //自定义的FlatMapFunction
    public static class MyFlapMapFunc implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            //按照空格切分value
            String[] words = value.split(" ");
            //遍历words输出数据
            for (String word : words) {
                //.collect是迭代器的方法吗？
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
