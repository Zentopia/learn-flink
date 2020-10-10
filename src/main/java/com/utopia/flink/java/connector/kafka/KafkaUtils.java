package com.utopia.flink.java.connector.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class KafkaUtils {

    public static FlinkKafkaConsumer011<String> createKafkaConsumer(){
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.GROUP_ID_CONFIG);
        FlinkKafkaConsumer011<String> consumer011 = new FlinkKafkaConsumer011<>(KafkaConstants.SOURCE_TOPIC_NAME, new SimpleStringSchema(), props);
        consumer011.setStartFromLatest();
        return consumer011;
    }

    public static FlinkKafkaProducer011<String> createTripProducer() {
        return new FlinkKafkaProducer011<>(KafkaConstants.KAFKA_BROKERS, KafkaConstants.SINK_TOPIC_NAME, new SimpleStringSchema());
    }



}
