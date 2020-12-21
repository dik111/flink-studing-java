package com.example.apitest.sink;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * Desription:
 *
 * @ClassName SinkTest1Kafka
 * @Author Zhanyuwei
 * @Date 2020/12/21 10:23 下午
 * @Version 1.0
 **/
public class SinkTest1Kafka {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt");


        // 转换成sensor reading 类型
        DataStream<String> dataStream = inputStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String[] fields = s.split(",");

                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2])).toString();
            }
        });

        dataStream.addSink(new FlinkKafkaProducer<String>("jtbihdp01:9092","sinktest",new SimpleStringSchema()));

        env.execute();
    }
}
