package com.example.apitest.table;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

/**
 * Desription:
 *
 * @ClassName TableTest2CommonApi
 * @Author Zhanyuwei
 * @Date 2020/12/26 9:00 下午
 * @Version 1.0
 **/
public class TableTest2CommonApi {

    public static void main(String[] args) throws Exception {

        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env,oldStreamSettings);

        // 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env,blinkStreamSettings);

        // 基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        String filePath = "/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                .field("timestamp",DataTypes.BIGINT())
                .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");


        Table inputTable = tableEnv.from("inputTable");
        //tableEnv.toAppendStream(inputTable, Row.class).print();

        // 查询转换
        Table resultTable = inputTable.select("id,temperature")
                .filter("id = 'sensor_6' ");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count,temperature.avg as avgTemp");

        // SQL
        tableEnv.sqlQuery(" select id, temperature from inputTable where id = 'sensor_6' ");
        Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) as avgTemp from inputTable group by id");

        // 打印输出
        tableEnv.toAppendStream(resultTable,Row.class).print("result");
        tableEnv.toRetractStream(sqlAggTable,Row.class).print("sqlAggTable");
        env.execute();

    }
}
