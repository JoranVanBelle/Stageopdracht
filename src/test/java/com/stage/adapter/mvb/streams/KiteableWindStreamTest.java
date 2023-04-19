package com.stage.adapter.mvb.streams;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.KafkaTopologyTestBase;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Set;

public class KiteableWindStreamTest extends KafkaTopologyTestBase {


    private static final String RAW_TOPIC = "raw";
    private static final String KITABLE_WIND_DETECTED_TOPIC = "wind";
	private static final String schema_registry = "mock://test";

    @BeforeEach
    void setup() {
        this.testDriver = createTestDriver(
        		KiteableWindStream.buildTopology(
                        Set.of("S1", "S2"),
                        7.717,
                        RAW_TOPIC,
                        KITABLE_WIND_DETECTED_TOPIC,
                        MergedWeatherStream.rawDataMeasuredSerde(schema_registry),
                        MergedWeatherStream.kiteableWindDetectedSerde(schema_registry),
                        MergedWeatherStream.windHasFallenOffSerde(schema_registry),
                        serdesConfigTest()
                )
        );
    }
    
    @AfterEach
    void teardown() {
    	testDriver.close();
    }

    @Test
    public void testWindSpeedConstantlyGoesUp() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "m/s", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.kiteableWindDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, windDetectionsList.size());
        System.out.println(windDetectionsList.get(0));

    }

    @Test
    public void testWindSpeedConstantlyGoesDown() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 9L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readKeyValue();

//        Assertions.assertEquals(2, windDetectionsList.size());
        System.err.println(windDetectionsList);

    }
    
    @Test
    public void testWindSpeedAlwaysTooLow() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(1, windDetectionsList.size());

    }
    
    @Test
    public void testWindSpeedAlwaysKiteable() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "19.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.72", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 5L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(1, windDetectionsList.size());

    }
    
    @Test
    public void testWindSpeedFluctuating() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "11.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 11L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 13L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 14L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 15L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 16L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(7, windDetectionsList.size());

    }
    
    @Test
    public void testWindSpeedConstantlyGoesUpTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "m/s", 10L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "4.00", "m/s", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "6.00", "m/s", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "7.00", "m/s", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "8.00", "m/s", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "9.00", "m/s", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "10.00", "m/s", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.kiteableWindDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(4, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindSpeedConstantlyGoesDownTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 9L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "10.00", "m/s", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "9.00", "m/s", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "8.00", "m/s", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "7.00", "m/s", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "6.00", "m/s", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "5.00", "m/s", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "4.00", "m/s", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "3.00", "m/s", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "2.00", "m/s", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "1.00", "m/s", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(4, windDetectionsList.size());

    }
    
    @Test
    public void testWindSpeedAlwaysTooLowTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 10L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "4.00", "m/s", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "5.00", "m/s", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "6.00", "m/s", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "7.00", "m/s", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "6.00", "m/s", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "5.00", "m/s", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "4.00", "m/s", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(2, windDetectionsList.size());

    }

    @Test
    public void testWindSpeedAlwaysKiteableTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "19.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.72", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 5L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "8.00", "m/s", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "9.00", "m/s", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "19.00", "m/s", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "7.72", "m/s", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "9.00", "m/s", 5L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(2, windDetectionsList.size());

    }
    
    @Test
    public void testWindSpeedFluctuatingTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "m/s", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "m/s", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "11.00", "m/s", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "6.00", "m/s", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "5.00", "m/s", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "4.00", "m/s", 11L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "2.00", "m/s", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 13L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 14L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "m/s", 15L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "m/s", 16L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "1.00", "m/s", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "2.00", "m/s", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "3.00", "m/s", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "8.00", "m/s", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "9.00", "m/s", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "10.00", "m/s", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "11.00", "m/s", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "6.00", "m/s", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "5.00", "m/s", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "4.00", "m/s", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "4.00", "m/s", 11L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "2.00", "m/s", 12L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "8.00", "m/s", 13L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "7.00", "m/s", 14L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "8.00", "m/s", 15L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "7.00", "m/s", 16L));
        
        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.windHasFallenOffSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(14, windDetectionsList.size());

    }
    
}
