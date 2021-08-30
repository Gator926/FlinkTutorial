package com.bigdata.cdc;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkSQLCDC {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String sql = "CREATE TABLE user_info (" +
                "id STRING PRIMARY KEY," +
                "name STRING," +
                "sex STRING" +
                ") WITH (" +
                " 'connector' = 'mysql-cdc'," +
                " 'scan.startup.mode' = 'latest-offset'," +
                " 'hostname' = 'localhost'," +
                " 'port' = '3306'," +
                " 'username' = 'root'," +
                " 'password' = 'root'," +
                " 'database-name' = 'cdc_test'," +
                " 'table-name' = 'user_info'" +
                ")";
        tableEnv.executeSql(sql);

        // 查询数据并转换为输出流
        Table table = tableEnv.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);
        retractStream.print();

        env.execute();
    }
}
