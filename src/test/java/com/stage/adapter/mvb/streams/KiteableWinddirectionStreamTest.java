package com.stage.adapter.mvb.streams;

import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.KafkaTopologyTestBase;

public class KiteableWinddirectionStreamTest extends KafkaTopologyTestBase {

    private static final String RAW_TOPIC = "raw";
    private static final String KITABLE_WIND_DETECTED_TOPIC = "wind.direction";
	private static final String schema_registry = "mock://test";
	
    @BeforeEach
    void setup() {
    	this.testDriver = createTestDriver(
    			KiteableWinddirectionStream.buildTopology(
    					Map.of("S1", new double[] {10.00, 100.00}, "S2", new double[] {50.00, 130.00}),
    					RAW_TOPIC,
    					KITABLE_WIND_DETECTED_TOPIC,
    					KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry),
                        KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry),
                        KiteableWinddirectionStream.unkiteableWindDirectionDetected(schema_registry),
    					serdesConfigTest()
				)
		);
    }
    
    @AfterEach
    void teardown() {
    	testDriver.close();
    }
    
    @Test
    public void testWindDirectionConstantlyGoesUpStartingOutBounderies() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(3, windDetectionsList.size());
    }
    
    @Test
    public void testWindDirectionConstantlyGoesUpStartingInBounderies() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionConstantlyGoesDownStartingOutBounderies() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(3, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionConstantlyGoesDownStartingInBounderies() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionAlwaysUnkiteable() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "101.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "200.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "250.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "275.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "360.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(1, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionAlwaysKiteable() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "91.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "95.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(1, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionFluctuating() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 11L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 13L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 14L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 15L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "deg", 16L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.unkiteableWindDirectionDetected(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(7, windDetectionsList.size());

    }
    
    @Test
    public void testWindDirectionConstantlyGoesUpStartingOutBounderiesTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 11L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "9.00", "deg", 0L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "10.00", "deg", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "20.00", "deg", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "30.00", "deg", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "40.00", "deg", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "60.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120.00", "deg", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "140.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(6, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionConstantlyGoesUpStartingInBounderiesTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 11L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "60.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120.00", "deg", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "140.00", "deg", 11L));
        
        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(4, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionConstantlyGoesDownStartingOutBounderiesTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 11L));

        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "140.00", "deg", 0L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "130.00", "deg", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120.00", "deg", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "110.00", "deg", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "90.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "80.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "60.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "40.00", "deg", 10L));
        
        

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(6, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionConstantlyGoesDownStartingInBounderiesTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 11L));

        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "130.00", "deg", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120.00", "deg", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "110.00", "deg", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "90.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "80.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "70.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "60.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "40.00", "deg", 10L));
        

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(4, windDetectionsList.size());
    }
    
    @Test
    public void testWindDirectionAlwaysUnkiteableTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "101.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "200.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "250.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "275.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "360.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "8.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 11L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "131.00", "deg", 0L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "140.00", "deg", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "150.00", "deg", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160.00", "deg", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "200.00", "deg", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "230.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "260.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "280.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "300.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "360.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "30.00", "deg", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "49.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionAlwaysKiteableTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 0L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "60.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "70.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "91.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "95.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 11L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 0L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "60.00", "deg", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "70.00", "deg", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "80.00", "deg", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "90.00", "deg", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "110.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "130.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "60.00", "deg", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 11L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.kiteableWindDirectionDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindDirectionFluctuatingTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWinddirectionStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "1.00", "deg", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "30.00", "deg", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "40.00", "deg", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "50.00", "deg", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110.00", "deg", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100.00", "deg", 11L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "90.00", "deg", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "20.00", "deg", 13L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "10.00", "deg", 14L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "9.00", "deg", 15L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "7.00", "deg", 16L));
        
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "1.00", "deg", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "10.00", "deg", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "20.00", "deg", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "9.00", "deg", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "30.00", "deg", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "40.00", "deg", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "50.00", "deg", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "80.00", "deg", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "110.00", "deg", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100.00", "deg", 11L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "90.00", "deg", 12L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "20.00", "deg", 13L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "10.00", "deg", 14L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "9.00", "deg", 15L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "7.00", "deg", 16L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWinddirectionStream.unkiteableWindDirectionDetected(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(10, windDetectionsList.size());
    }
    

    @Test
    public void testWaveHeightStream_MeasuredSameValue() {

        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), KiteableWaveStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));

        var waveDetectionsList = testDriver.createOutputTopic(KITABLE_WIND_DETECTED_TOPIC, new StringDeserializer(), KiteableWaveStream.kiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(1, waveDetectionsList.size());
    }
}
