package com.stage.adapter.mvb.streams;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.KafkaTopologyTestBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

public class KiteableWindStreamTest extends KafkaTopologyTestBase {


    private static final String RAW_TOPIC = "raw";
    private static final String KITABLE_WIND_DETECTED_TOPIC = "wind";

    @BeforeEach
    void setup() {
        this.testDriver = createTestDriver(
                KiteableWindStream.buildTopology(
                        Set.of("S1"),
                        5,
                        RAW_TOPIC,
                        KITABLE_WIND_DETECTED_TOPIC,
                        KiteableWindStream.rawDataMeasuredSerde(serdesConfigTest()),
                        KiteableWindStream.kiteableWindDetectedSerde(serdesConfigTest()),
                        serdesConfigTest()
                )
        );
    }

    @Test
    public void testWindSpeedConstantlyGoesUp() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWindStream.rawDataMeasuredSerde(serdesConfigTest()).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "knts", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "knts", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "knts", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "knts", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "knts", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "knts", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "knts", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "knts", 8L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWindStream.kiteableWindDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();

        Assertions.assertEquals(1, windDetectionsList.size());

    }


}
