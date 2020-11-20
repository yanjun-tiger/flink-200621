package com.atguigu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author zhouyanjun
 * @create 2020-11-18 14:48
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2
        DataStreamSource<String> fileDS = env.readTextFile("sensor");
        //3 把数据写入MySQL
        fileDS.addSink(new JdbcSink());
        //4 执行
        env.execute();
    }

    public static class JdbcSink extends RichSinkFunction<String> {
        //声明mysql的属性信息
        //连接信息 导入import java.sql.Connection; 为什么不是jdbc的connection？
        Connection connection = null;
        //预声明语句 导入java.sql why？
        PreparedStatement preparedStatement = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test",
                    "root", "123456");
            preparedStatement
                    = connection.prepareStatement("insert into sensor(id,temp) values(?,?) on duplicate key update temp=?");
        }

        @Override
        //why?这个是处理每条数据
        public void invoke(String value, Context context) throws Exception {
            String[] words = value.split(",");

            //给预编译SQL赋值
            preparedStatement.setString(1, words[0]);
            preparedStatement.setDouble(2, Double.parseDouble(words[2]));
            preparedStatement.setDouble(3, Double.parseDouble(words[2]));
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
