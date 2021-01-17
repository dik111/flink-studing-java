package com.example.apitest.window;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Desription:
 *
 * @ClassName WindowTest3EventTimeWindow
 * @Author Zhanyuwei
 * @Date 2021/1/13 9:34 下午
 * @Version 1.0
 **/
public class WindowTest3EventTimeWindow {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);

        // socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);
        // 转换成sensor reading 类型,分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(line ->{
            String[] fields = line.split(",");

            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 基于事件时间的开窗聚合,统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> minTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(15))
                .minBy("temperature");

        minTempStream.print("minTemp");


        env.execute();

    }
}
