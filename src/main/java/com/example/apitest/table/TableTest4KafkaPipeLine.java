package com.example.apitest.table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;

/**
 * Desription:
 *
 * @ClassName TableTest4KafkaPipeLine
 * @Author Zhanyuwei
 * @Date 2020/12/27 12:51 下午
 * @Version 1.0
 **/
public class TableTest4KafkaPipeLine {

    public static void main(String[] args) throws Exception {
        // 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 连接kafka，读取数据
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sensor2")
                .property("zookeeper.connect","jtbihdp03:2181")
                .property("bootstrap.servers","jtbihdp03:9092")
                        )
                .withFormat(new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("inputTable");

        // 简单转换
        Table inputTable = tableEnv.from("inputTable");
        //tableEnv.toAppendStream(inputTable, Row.class).print();

        // 查询转换
        Table resultTable = inputTable.select("id,temperature")
                .filter("id = 'sensor_6' ");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id,id.count as count,temperature.avg as avgTemp");

        // SQL
        //tableEnv.sqlQuery(" select id, temperature from inputTable where id = 'sensor_6' ");
        //Table sqlAggTable = tableEnv.sqlQuery("select id,count(id) as cnt,avg(temperature) as avgTemp from inputTable group by id");

        // 建立kafka 连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("universal")
                .topic("sinkTest2")
                .property("zookeeper.connect","jtbihdp03:2181")
                .property("bootstrap.servers","jtbihdp03:9092")
        )
                .withFormat(new Csv())
                .withSchema( new Schema()
                        .field("id", DataTypes.STRING())
                        //.field("timestamp",DataTypes.BIGINT())
                        .field("temperature",DataTypes.DOUBLE()))
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
        env.execute();
    }
}
