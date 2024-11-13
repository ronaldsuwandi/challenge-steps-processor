package com.ronaldsuwandi;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Main {
    private static Logger logger = LoggerFactory.getLogger(Main.class);

    private static Properties init() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "steps-processor");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "steps-processor-client");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        String stateDir = System.getenv().getOrDefault("KAFKA_STREAMS_STATE_DIR", "./store");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // disable caching
        return props;
    }

    public static void main(String[] args) {
        Properties props = init();

        TopologyBuilder topologyBuilder = new TopologyBuilder();
        Topology topology = topologyBuilder.build();
        System.out.println(topology.describe());
        KafkaStreams streams = new KafkaStreams(topology, props);

        streams.setStateListener((state, state1) -> {
            if (state1.isRunningOrRebalancing()) {
                logger.info("Kafka Streams is READY");
            }
        });

        // start stream
        streams.start();

        // Add shutdown hook for clean shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
