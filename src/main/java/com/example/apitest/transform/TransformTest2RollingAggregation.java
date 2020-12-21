package com.example.apitest.transform;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desription:
 *
 * @ClassName TransformTest2RollingAggregation
 * @Author Zhanyuwei
 * @Date 2020/12/17 8:53 下午
 * @Version 1.0
 **/
public class TransformTest2RollingAggregation {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = executionEnvironment.readTextFile("/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt");

        // 转换成sensor reading 类型
        //DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
        //    @Override
        //    public SensorReading map(String s) throws Exception {
        //        String[] fields = s.split(",");
        //
        //        return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        //    }
        //});

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 分组
        KeyedStream<SensorReading, Tuple> keyedStream = dataStream.keyBy("id");
        //KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(SensorReading::getId);

        // 滚动聚合，取当前最大的温度值
        SingleOutputStreamOperator<SensorReading> resultStream = keyedStream.max("temperature");

        resultStream.print();

        executionEnvironment.execute();
    }
}
