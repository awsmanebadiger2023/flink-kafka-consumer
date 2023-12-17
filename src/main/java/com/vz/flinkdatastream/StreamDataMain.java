package com.vz.flinkdatastream;


import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamDataMain {
    static String TOPIC_IN = "adapt-inbound";
    static String BOOTSTRAP_SERVER = "adapt-server:9092,adapt-server:9093,adapt-server:9094";
    static Logger logger = LoggerFactory.getLogger(StreamDataMain.class);
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);

        FlinkConsumer consumer = new FlinkConsumer();
        logger.info("##############################Starting the Consumer##############################");
        logger.debug("@@@@@@@@@@@@@@@@@@@@@EXTRA DEBUG OUTPUT@@@@@@@@@@@@@@@@@@@@@");
        try {
            consumer.start(env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}