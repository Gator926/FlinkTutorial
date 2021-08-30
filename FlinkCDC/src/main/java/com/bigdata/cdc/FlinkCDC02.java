package com.bigdata.cdc;

import com.bigdata.cdc.func.CustomerDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkCDC02 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 开启checkpoint
//        env.enableCheckpointing(5000);
//        env.getCheckpointConfig().setAlignmentTimeout(10000);
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//
//        env.setStateBackend(new FsStateBackend("file:///C:\\Code\\FlinkTutorial\\FlinkCDC\\src\\main\\checkpoints"));

        // 通过FlinkCDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("localhost")
                .serverTimeZone("Asia/Shanghai")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("cdc_test")
                .deserializer(new CustomerDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 数据打印
        streamSource.print();

        // 启动任务
        env.execute();
    }
}
