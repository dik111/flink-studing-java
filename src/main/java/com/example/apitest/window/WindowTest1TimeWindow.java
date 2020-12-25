package com.example.apitest.window;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Desription:
 *
 * @ClassName WindowTest1TimeWindow
 * @Author Zhanyuwei
 * @Date 2020/12/23 11:15 下午
 * @Version 1.0
 **/
public class WindowTest1TimeWindow {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt");


        // 转换成sensor reading 类型
        DataStream<SensorReading> dataStream = inputStream.map(line ->{
            String[] fields = line.split(",");

            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 开窗测试
        SingleOutputStreamOperator<Integer> resultStream = (SingleOutputStreamOperator<Integer>) dataStream.keyBy("id")
                //.window(TumblingProcessingTimeWindows.of(Time.seconds(15)));
                .timeWindow(Time.seconds(15))
                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorReading sensorReading, Integer integer) {
                        return integer + 1;
                    }

                    @Override
                    public Integer getResult(Integer integer) {
                        return integer;
                    }

                    @Override
                    public Integer merge(Integer integer, Integer acc1) {
                        return integer + acc1;
                    }
                });
        resultStream.print();
        env.execute();
    }
}
