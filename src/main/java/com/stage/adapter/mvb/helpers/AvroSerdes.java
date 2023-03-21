package com.stage.adapter.mvb.helpers;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.specific.SpecificRecord;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class AvroSerdes {

	public static <T extends SpecificRecord> SpecificAvroSerde<T> get() {
		var specificSerdes = new SpecificAvroSerde<T>();
		
		Map<String, Object> propMap = new HashMap<>();
		propMap.put("SCHEMA_REGISTRY_URL_CONFIG", "http://localhost:8081/subjects/_mvbschemas/versions");
		
		specificSerdes.configure(propMap, false);
		return specificSerdes;
	}


	
}
