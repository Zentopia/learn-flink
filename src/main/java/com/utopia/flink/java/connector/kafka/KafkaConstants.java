package com.utopia.flink.java.connector.kafka;

public class KafkaConstants {
    public static final String KAFKA_BROKERS = "192.168.10.36:6667";

    public static final String KAFKA_BROKERS_TEST = "182.106.128.151:9092";

    public static final String CLIENT_ID = "realtime-bigdata-client";

    public static final String SOURCE_TOPIC_NAME = "car_input_data";

    public static final String SINK_TOPIC_NAME = "car_output_data";

    public static final String GROUP_ID_CONFIG = "realtime-bigdata-group";

    public static final Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;

    public static final String OFFSET_RESET_LATEST = "latest";

    public static final String OFFSET_RESET_EARLIER = "earliest";

    public static final Integer MAX_POLL_RECORDS = 1;

    public final Integer MAX_REQUEST_SIZE = 524288000;
}
