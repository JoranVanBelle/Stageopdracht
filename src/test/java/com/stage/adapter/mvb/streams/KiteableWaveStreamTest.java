package com.stage.adapter.mvb.streams;

import java.util.Set;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.KafkaTopologyTestBase;

public class KiteableWaveStreamTest extends KafkaTopologyTestBase {
	
	private static final String RAW_TOPIC = "raw";
	private static final String KITABLE_WAVE_DETECTED_TOPIC = "wave";
	private static final String schema_registry = "mock://test";
	
	@BeforeEach
	void setup() {
		this.testDriver = createTestDriver(
				KiteableWaveStream.buildTopology(
						Set.of("S1", "S2"),
						150,
						RAW_TOPIC,
						KITABLE_WAVE_DETECTED_TOPIC,
						MergedWeatherStream.rawDataMeasuredSerde(schema_registry),
						MergedWeatherStream.kiteableWaveDetectedSerde(schema_registry),
						MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry),
						serdesConfigTest(),
						new StreamsBuilder()
				).build()
		);
	}
	
	@AfterEach
	void teardown() {
		testDriver.close();
	}
	
	@Test
	public void testWaveHeightConstantlyUp() {
		
		var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
		
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 10L));
		
        var waveDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.kiteableWindDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, waveDetectionsList.size());
	}
	
    @Test
    public void testWindSpeedConstantlyGoesDown() {
    	
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
        
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 9L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(2, windDetectionsList.size());

    }
    
    @Test
    public void testWaveHeightAlwaysTooLow() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());

        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(1, windDetectionsList.size());
    }
    
    @Test
    public void testWaveHeightAlwaysKiteable() {
		
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
    	
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "200", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(1, windDetectionsList.size());
    }
    
    @Test
    public void testWaveHeightFluctuating() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
		
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 13L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 14L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 15L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 16L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 17L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 18L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 19L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(7, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindSpeedConstantlyGoesUpTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
		
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 10L));
		
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "100", "cm", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "110", "cm", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "120", "cm", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "130", "cm", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "140", "cm", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "150", "cm", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "160", "cm", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "170", "cm", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "180", "cm", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "190", "cm", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(4, windDetectionsList.size());
        
    }
    
    @Test
    public void testWindSpeedConstantlyGoesDownTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
		
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 10L));
		
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "190", "cm", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "180", "cm", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "170", "cm", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "160", "cm", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "150", "cm", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "140", "cm", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "130", "cm", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "120", "cm", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "110", "cm", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "100", "cm", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(4, windDetectionsList.size());
        
    }

    @Test
    public void testWindSpeedConstantlyTooLowTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
		
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 10L));
		
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "100", "cm", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "110", "cm", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "120", "cm", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "130", "cm", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "140", "cm", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "150", "cm", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "140", "cm", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "130", "cm", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "120", "cm", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten elders", "110", "cm", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();
        
        Assertions.assertEquals(2, windDetectionsList.size());
        
    }
    
    @Test
    public void testWaveHeightAlwaysKiteableTwoSensors() {
		
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
    	
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "200", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 10L));
    	
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "200", "cm", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "190", "cm", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "180", "cm", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "170", "cm", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160", "cm", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160", "cm", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "190", "cm", 10L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(2, windDetectionsList.size());
    }
    
    @Test
    public void testWaveHeightFluctuatingTwoSensors() {
        var rawMeasurements = testDriver.createInputTopic(RAW_TOPIC, new StringSerializer(), MergedWeatherStream.rawDataMeasuredSerde(schema_registry).serializer());
		
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 10L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "180", "cm", 12L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "170", "cm", 13L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 14L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "150", "cm", 15L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "160", "cm", 16L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "140", "cm", 17L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "190", "cm", 18L));
        rawMeasurements.pipeInput("S1", new RawDataMeasured("S1", "Twoasten", "100", "cm", 19L));
		
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100", "cm", 1L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "110", "cm", 2L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "120", "cm", 3L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "130", "cm", 4L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "140", "cm", 5L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "150", "cm", 6L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160", "cm", 7L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "170", "cm", 8L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "180", "cm", 9L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "190", "cm", 10L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "190", "cm", 12L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "180", "cm", 12L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "170", "cm", 13L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160", "cm", 14L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "150", "cm", 15L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "160", "cm", 16L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "140", "cm", 17L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "190", "cm", 18L));
        rawMeasurements.pipeInput("S2", new RawDataMeasured("S2", "Twoasten", "100", "cm", 19L));

        var windDetectionsList = testDriver.createOutputTopic(KITABLE_WAVE_DETECTED_TOPIC, new StringDeserializer(), MergedWeatherStream.unkiteableWaveDetectedSerde(schema_registry).deserializer()).readRecordsToList();

        Assertions.assertEquals(14, windDetectionsList.size());
        
    }
    
}
