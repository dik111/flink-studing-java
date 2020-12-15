package com.example.source;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * Desription:
 *
 * @ClassName SourceTest1Collection
 * @Author Zhanyuwei
 * @Date 2020/12/15 10:07 下午
 * @Version 1.0
 **/
public class SourceTest1Collection {

    public static void main(String[] args) throws Exception{

        // 创建执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 从集合中读取数据
        DataStream<SensorReading> dataStream = environment.fromCollection(Arrays.asList(new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_7", 1547718202L, 6.7),
                new SensorReading("sensor_10", 1547718205L, 38.1)));

        DataStreamSource<Integer> integerDataStreamSource = environment.fromElements(1, 2, 3, 67);

        dataStream.print("data");
        integerDataStreamSource.print("int");

        // 执行
        environment.execute();
    }
}
