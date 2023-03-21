package com.stage.adapter.mvb;

import static io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;

public class KafkaTopologyTestBase {
	
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://test";
    protected TopologyTestDriver testDriver;


    protected static TopologyTestDriver createTestDriver(Topology topology) {
        // Dummy properties needed for test diver
        Properties topologyConfig = new Properties();
        topologyConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        topologyConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
//        topologyConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
//        topologyConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        topologyConfig.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        topologyConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        return new TopologyTestDriver(topology, topologyConfig);
    }

    protected static Properties serdesConfigTest(){
        Properties settings = new Properties();
        settings.put(SCHEMA_REGISTRY_URL_CONFIG,MOCK_SCHEMA_REGISTRY_URL);
        
        return settings;
    }


}
