package com.example.apitest.table;

import com.example.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * Desription:
 *
 * @ClassName TableTest1Example
 * @Author Zhanyuwei
 * @Date 2020/12/26 12:40
 * @Version 1.0
 **/
public class TableTest1Example {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        // 从文件读取数据
        DataStream<String> inputStream = env.readTextFile("E:\\java\\flink-studing-java\\src\\main\\resources\\sensor.txt");

        // 转换成sensor reading 类型
        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String s) throws Exception {
                String[] fields = s.split(",");

                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
            }
        });

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于流创建一张表
        Table dataTable = tableEnv.fromDataStream(dataStream);

        // 调用table API进行转换操作
        Table resultTable = dataTable.select("id,temperature")
                .where("id = 'sensor_1' ");


        // 执行SQL
        tableEnv.createTemporaryView("sensor",dataTable);
        String sql = " select id,temperature from sensor where id = 'sensor_1' ";
        Table resultSqlTable = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(resultTable, Row.class).print("result");
        tableEnv.toAppendStream(resultSqlTable, Row.class).print("sql");

        env.execute();
    }
}
