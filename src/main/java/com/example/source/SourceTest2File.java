package com.example.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desription:
 *
 * @ClassName SourceTest2File
 * @Author Zhanyuwei
 * @Date 2020/12/16 9:09 下午
 * @Version 1.0
 **/
public class SourceTest2File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 从文件读取数据
        DataStream<String> dataStream = executionEnvironment.readTextFile("/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt");

        dataStream.print();

        executionEnvironment.execute();
    }
}
