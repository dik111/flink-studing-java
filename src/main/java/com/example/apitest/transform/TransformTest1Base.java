package com.example.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * Desription:
 *
 * @ClassName TransformTest1Base
 * @Author Zhanyuwei
 * @Date 2020/12/16 9:47 下午
 * @Version 1.0
 **/
public class TransformTest1Base {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = executionEnvironment.readTextFile("/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt");

        // 1.map,把string转换成长度输出
        DataStream<Integer> mapStream = inputStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        // 2.flatmap,按逗号分字段
        DataStream<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");
                for (String field : fields) {
                    collector.collect(field);
                }
            }
        });

        // 3.filter,筛选sensor_1开头的数据
        DataStream<String> filterStream = inputStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("sensor_1");
            }
        });
        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        executionEnvironment.execute();
    }
}
