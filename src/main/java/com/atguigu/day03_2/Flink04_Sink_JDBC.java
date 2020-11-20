package com.atguigu.day03_2;

import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhouyanjun
 * @create 2020-11-18 18:39
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1 创造执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2 读取数据
        DataStreamSource<String> fileDS = env.readTextFile("sensor");
        //3 往mysql里写数据
        fileDS.addSink(new MyJdbcSink());
        //4 执行
        env.execute();
    }

    public static class MyJdbcSink extends RichSinkFunction<String> {
        //声明配置jdbc的属性
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection
                    = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test","root","123456");
            preparedStatement
                    = connection.prepareStatement("insert into sensor(id,temp) values(?,?) on duplicate key update temp=?");
        }

        @Override
        //why? 这个是处理每一行数据？
        public void invoke(String value, Context context) throws Exception {
            String[] words = value.split(",");

            preparedStatement.setString(1,words[0]);
            preparedStatement.setDouble(2,Double.parseDouble(words[2]));
            preparedStatement.setDouble(3,Double.parseDouble(words[2]));
            //执行
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            preparedStatement.close();
            connection.close();
        }

    }
}
