package com.vz.flinkdatastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class FlinkConsumer {
    static Logger logger = LoggerFactory.getLogger(FlinkConsumer.class);
     String TOPIC_IN;
     String BOOTSTRAP_SERVER;
     String GROUP_NAME;

    public FlinkConsumer(String TOPIC_IN, String BOOTSTRAP_SERVER,String GROUP_NAME) {
        this.TOPIC_IN = TOPIC_IN;
        this.BOOTSTRAP_SERVER = BOOTSTRAP_SERVER;
        this.GROUP_NAME = GROUP_NAME;
    }

    public void start(StreamExecutionEnvironment env) throws Exception {
        env.enableCheckpointing(100, CheckpointingMode.AT_LEAST_ONCE);
        KafkaSource<String> source =   KafkaSource.<String>builder()
                .setBootstrapServers(this.BOOTSTRAP_SERVER)
                .setTopics(TOPIC_IN)
                .setGroupId(GROUP_NAME)
                .setProperty("enable.auto.commit", "false")
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        dataStream.print();
        logger.info("OBJECT RECEIVED FROM CONSUMER IS:{}",dataStream.print());
        env.execute("ADAPT-CONSUMER");
    }
}
