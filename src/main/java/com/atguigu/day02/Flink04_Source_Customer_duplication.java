package com.atguigu.day02;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author zhouyanjun
 * @create 2020-11-17 14:32
 */
public class Flink04_Source_Customer_duplication {
    public static void main(String[] args) {
        //1 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2 从自定义的数据源中读取数据
        DataStreamSource<Object> mySourceDS = env.addSource(new CustomerSource());
        //4 测试、打印
        //5 启动任务
    }

    //什么时候假如泛型呢？要看实现类，如果实现类里明确标注泛型，就必须写泛型。下行代码没有写泛型，上面代码标黄，没有变红
    public static class CustomerSource implements SourceFunction {
        @Override
        public void run(SourceContext sourceContext) throws Exception {

        }

        @Override
        public void cancel() {

        }
    }
}
