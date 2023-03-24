package com.stage.adapter.mvb.streams;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.provider.CsvSource;

import com.stage.KiteableWaveDetected;
import com.stage.KiteableWindDetected;
import com.stage.KiteableWindDirectionDetected;
import com.stage.UnkiteableWaveDetected;
import com.stage.UnkiteableWindDetected;
import com.stage.UnkiteableWindDirectionDetected;
import com.stage.adapter.mvb.KafkaTopologyTestBase;

public class KiteableWeatherStreamTest extends KafkaTopologyTestBase {
	
	private static final String WIND_SPEED_TOPIC = "windspeed";
	private static final String WAVE_HEIGHT_TOPIC = "waveheight";
	private static final String WIND_DIRECTION_TOPIC = "winddirection";
	private static final String OUTPUT_TOPIC = "kiteable";
	
	@BeforeEach
	void setup() {		
		this.testDriver = createTestDriver(
				 KiteableWeatherStream.buildTopology(
							WIND_SPEED_TOPIC,
							WAVE_HEIGHT_TOPIC,
							WIND_DIRECTION_TOPIC,
							OUTPUT_TOPIC,
							KiteableWeatherStream.genericRecordSerde(serdesConfigTest()),
							KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()),
							KiteableWeatherStream.noKiteableWeatherDetectedSerde(serdesConfigTest()),
							serdesConfigTest()
							)
				 );
	}
	
	@AfterEach
	void teardown() {
		testDriver.close();
	}
	
	@Test
	public void testOutputKiteableEvent() {
		
		var windSpeedTopic = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDetectedSerde(serdesConfigTest()).serializer());
		var waveHeightTopic = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopic = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
						
		windSpeedTopic.pipeInput("NP7WC3", new KiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 1L));
		waveHeightTopic.pipeInput("NPBGHA", new KiteableWaveDetected("NPBGHA", "Twoasten", "151.00", "cm", 1L));
		windDirectionTopic.pipeInput("NP7WRS", new KiteableWindDirectionDetected("NP7WRS", "Twoasten", "10.00", "deg", 1L));
		
		var kiteableEvent = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readKeyValue();
		
		Assertions.assertEquals("NieuwpoortKiteable1", kiteableEvent.value.getDataID());
		Assertions.assertEquals("Nieuwpoort", kiteableEvent.value.getLocatie());
		Assertions.assertEquals("9.00", kiteableEvent.value.getWindsnelheid());
		Assertions.assertEquals("m/s", kiteableEvent.value.getEenheidWindsnelheid());
		Assertions.assertEquals("151.00", kiteableEvent.value.getGolfhoogte());
		Assertions.assertEquals("cm", kiteableEvent.value.getEenheidGolfhoogte());
		Assertions.assertEquals("10.00", kiteableEvent.value.getWindrichting());
		Assertions.assertEquals("deg", kiteableEvent.value.getEenheidWindrichting());
		Assertions.assertEquals(1, kiteableEvent.value.getTijdstip());
	}
	
	@Test
	public void testOutputUnkiteableEvent() {
		
		var windSpeedTopic = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.windHasFallenOffSerde(serdesConfigTest()).serializer());
		var waveHeightTopic = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopic = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
						
		windSpeedTopic.pipeInput("NP7WC3", new UnkiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 1L));
		waveHeightTopic.pipeInput("NPBGHA", new KiteableWaveDetected("NPBGHA", "Twoasten", "151.00", "cm", 1L));
		windDirectionTopic.pipeInput("NP7WRS", new KiteableWindDirectionDetected("NP7WRS", "Twoasten", "10.00", "deg", 1L));
		
		var unkiteableEvent = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.noKiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readKeyValue();
		var kiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();
		
		Assertions.assertEquals("NieuwpoortUnkiteable1", unkiteableEvent.value.getDataID());
		Assertions.assertEquals("Nieuwpoort", unkiteableEvent.value.getLocatie());
		Assertions.assertEquals("9.00", unkiteableEvent.value.getWindsnelheid());
		Assertions.assertEquals("m/s", unkiteableEvent.value.getEenheidWindsnelheid());
		Assertions.assertEquals("151.00", unkiteableEvent.value.getGolfhoogte());
		Assertions.assertEquals("cm", unkiteableEvent.value.getEenheidGolfhoogte());
		Assertions.assertEquals("10.00", unkiteableEvent.value.getWindrichting());
		Assertions.assertEquals("deg", unkiteableEvent.value.getEenheidWindrichting());
		Assertions.assertEquals(1, unkiteableEvent.value.getTijdstip());
		
		Assertions.assertEquals(0, kiteableEventList.size());
	}
	
	@Test
	public void testAmountOfResultEvents() {
		var windSpeedTopicKiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDetectedSerde(serdesConfigTest()).serializer());
		var windSpeedTopicUnkiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.windHasFallenOffSerde(serdesConfigTest()).serializer());
		var waveHeightTopicKiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var waveHeightTopicUnkiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicKiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicUnkiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
		
		// NO
		windSpeedTopicKiteable.pipeInput("NP7WC3", new KiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 1L));
		windSpeedTopicUnkiteable.pipeInput("NP7WC3", new UnkiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 2L));
		waveHeightTopicKiteable.pipeInput("NPBGHA", new KiteableWaveDetected("NP7WC3", "Twoasten", "151.00", "cm", 1L));
		windDirectionTopicKiteable.pipeInput("NP7WRS", new KiteableWindDirectionDetected("NP7WC3", "Twoasten", "10.00", "deg", 1L));
		
		// YES
		windSpeedTopicKiteable.pipeInput("NP7WC3", new KiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 3L));
		
		// NO
		waveHeightTopicUnkiteable.pipeInput("NPBGHA", new UnkiteableWaveDetected("NP7WC3", "Twoasten", "151.00", "cm", 2L));
		
		// NO
		windDirectionTopicUnkiteable.pipeInput("NP7WRS", new UnkiteableWindDirectionDetected("NP7WC3", "Twoasten", "10.00", "deg", 2L));
		
		// YES
		waveHeightTopicKiteable.pipeInput("NPBGHA", new KiteableWaveDetected("NP7WC3", "Twoasten", "151.00", "cm", 3L));
		windDirectionTopicKiteable.pipeInput("NP7WRS", new KiteableWindDirectionDetected("NP7WC3", "Twoasten", "10.00", "deg", 3L));
		
		
		var eventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();
		
		Assertions.assertEquals(5, eventList.size());
		
	}
	
	@Test
	public void publishNothingWhenOneKiteableParams() {
		var windSpeedTopicKiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDetectedSerde(serdesConfigTest()).serializer());
		var windSpeedTopicUnkiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.windHasFallenOffSerde(serdesConfigTest()).serializer());
		var waveHeightTopicKiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var waveHeightTopicUnkiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicKiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicUnkiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
	
		windSpeedTopicKiteable.pipeInput("NP7WC3", new KiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 1L));

		
		var kiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();
		var unkiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.noKiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();

		Assertions.assertEquals(0, kiteableEventList.size());
		Assertions.assertEquals(0, unkiteableEventList.size());
	}
	
	@Test
	public void publishNothingWhenTwoKiteableParams() {
		var windSpeedTopicKiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDetectedSerde(serdesConfigTest()).serializer());
		var windSpeedTopicUnkiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.windHasFallenOffSerde(serdesConfigTest()).serializer());
		var waveHeightTopicKiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var waveHeightTopicUnkiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicKiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicUnkiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
	
		windSpeedTopicKiteable.pipeInput("NP7WC3", new KiteableWindDetected("NP7WC3", "Twoasten", "9.00", "m/s", 1L));
		waveHeightTopicKiteable.pipeInput("NPBGHA", new KiteableWaveDetected("NPBGHA", "Twoasten", "151.00", "cm", 1L));

		
		var kiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();
		var unkiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.noKiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();

		Assertions.assertEquals(0, kiteableEventList.size());
		Assertions.assertEquals(0, unkiteableEventList.size());
	}
	
	@Test
	public void publishNothingWhenOneUnkiteableParams() {
		var windSpeedTopicKiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDetectedSerde(serdesConfigTest()).serializer());
		var windSpeedTopicUnkiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.windHasFallenOffSerde(serdesConfigTest()).serializer());
		var waveHeightTopicKiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var waveHeightTopicUnkiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicKiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicUnkiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
	
		waveHeightTopicUnkiteable.pipeInput("NPBGHA", new UnkiteableWaveDetected("NPBGHA", "Twoasten", "151.00", "cm", 1L));

		
		var kiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();
		var unkiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.noKiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();

		Assertions.assertEquals(0, kiteableEventList.size());
		Assertions.assertEquals(0, unkiteableEventList.size());
	}
	
	@Test
	public void publishNothingWhenTwoUnkiteableParams() {
		var windSpeedTopicKiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDetectedSerde(serdesConfigTest()).serializer());
		var windSpeedTopicUnkiteable = testDriver.createInputTopic(WIND_SPEED_TOPIC, new StringSerializer(), KiteableWeatherStream.windHasFallenOffSerde(serdesConfigTest()).serializer());
		var waveHeightTopicKiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var waveHeightTopicUnkiteable = testDriver.createInputTopic(WAVE_HEIGHT_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWaveDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicKiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.kiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
		var windDirectionTopicUnkiteable = testDriver.createInputTopic(WIND_DIRECTION_TOPIC, new StringSerializer(), KiteableWeatherStream.unkiteableWindDirectionDetectedSerde(serdesConfigTest()).serializer());
	
		waveHeightTopicUnkiteable.pipeInput("NPBGHA", new UnkiteableWaveDetected("NPBGHA", "Twoasten", "151.00", "cm", 1L));
		windDirectionTopicUnkiteable.pipeInput("NP7WRS", new UnkiteableWindDirectionDetected("NP7WRS", "Twoasten", "10.00", "deg", 1L));

		
		var kiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.kiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();
		var unkiteableEventList = testDriver.createOutputTopic(OUTPUT_TOPIC, new StringDeserializer(), KiteableWeatherStream.noKiteableWeatherDetectedSerde(serdesConfigTest()).deserializer()).readRecordsToList();

		Assertions.assertEquals(0, kiteableEventList.size());
		Assertions.assertEquals(0, unkiteableEventList.size());
	}
	
}
