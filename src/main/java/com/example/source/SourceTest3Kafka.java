package com.example.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * Desription:
 *
 * @ClassName SourceTest2File
 * @Author Zhanyuwei
 * @Date 2020/12/16 9:09 下午
 * @Version 1.0
 **/
public class SourceTest3Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","jtbihdp03:9092");
        // 从kafka读取数据
        DataStream<String> dataStream = executionEnvironment.addSource(new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties));

        dataStream.print();

        executionEnvironment.execute();
    }
}
