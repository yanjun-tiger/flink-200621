package com.atguigu.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author zhouyanjun
 * @create 2020-11-17 11:55
 */
@Data //就是get() set()
@NoArgsConstructor //空参构造器
@AllArgsConstructor
public class SensorReading {
    private String id;
    private Long ts;
    private Double temp;

    @Override
    public String toString() {
        return id + "," + ts + "," + temp;
    }
}
