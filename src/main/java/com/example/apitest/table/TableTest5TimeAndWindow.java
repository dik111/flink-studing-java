package com.example.apitest.table;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import javax.xml.bind.Element;

/**
 * Desription:
 *
 * @ClassName TableTest5TimeAndWindow
 * @Author Zhanyuwei
 * @Date 2020/12/29 8:51 下午
 * @Version 1.0
 **/
public class TableTest5TimeAndWindow {

    public static void main(String[] args) throws Exception{
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读入文件数据，得到DataStream
        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt");

        DataStream<SensorReading> dataStream = inputStream.map(line ->{
            String[] fields = line.split(",");

            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTimestamp() * 1000L;
            }
        });

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将流转换成表，定义时间特性
        //Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp as ts,temperature as temp,pt.proctime");
        Table dataTable = tableEnv.fromDataStream(dataStream, "id,timestamp.rowtime as ts,temperature as temp");

        tableEnv.toAppendStream(dataTable, Row.class).print();

        env.execute();
    }
}
