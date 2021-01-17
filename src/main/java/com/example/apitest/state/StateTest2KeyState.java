package com.example.apitest.state;

import apple.laf.JRSUIState;
import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * Desription:
 *
 * @ClassName StateTest1OperatorState
 * @Author Zhanyuwei
 * @Date 2021/1/16 9:53 下午
 * @Version 1.0
 **/
public class StateTest2KeyState {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket 文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);


        // 转换成sensor reading 类型
        DataStream<SensorReading> dataStream = inputStream.map(line ->{
            String[] fields = line.split(",");

            return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream
                .keyBy("id")
                .map(new MyKeyCountMapper());

        resultStream.print();

        env.execute();
    }

    // 自定义RichMapFunction
    public static class MyKeyCountMapper extends RichMapFunction<SensorReading,Integer>{

        private ValueState<Integer> keyCountState ;

        @Override
        public void open(Configuration parameters) throws Exception {
            keyCountState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("key-count",Integer.class));
        }

        @Override
        public Integer map(SensorReading sensorReading) throws Exception {
            Integer count = keyCountState.value();
            count++;
            keyCountState.update(count);
            return count;
        }
    }

}
