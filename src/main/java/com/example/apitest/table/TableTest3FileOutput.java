package com.example.apitest.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * Desription:
 *
 * @ClassName TableTest3FileOutput
 * @Author Zhanyuwei
 * @Date 2020/12/27 12:26 下午
 * @Version 1.0
 **/
public class TableTest3FileOutput {

    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

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

        // 输出到文件
        // 连接外部文件，注册输出表
        String outputPath = "/Users/yuwei1/Documents/java/project/studing-flink/src/main/resources/out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat( new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
        env.execute();

    }
}
