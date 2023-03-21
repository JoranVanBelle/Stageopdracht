package com.stage.adapter.mvb.processors;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KiteableWeatherDetected;
import com.stage.NoKiteableWeatherDetected;

public class UnkiteableWeatherReconProcessor implements Processor<String, GenericRecord, String, NoKiteableWeatherDetected> {

	private KeyValueStore<String, NoKiteableWeatherDetected> stateStoreUnkiteable;
	private KeyValueStore<String, KiteableWeatherDetected> stateStoreKiteable;
	private ProcessorContext<String, NoKiteableWeatherDetected> context;
	
	private String kvStoreNameUnkiteable;
	private String kvStoreNameKiteable;
	private String location;
	private String unkiteableKeyName;
	private String kiteableKeyName;
	
	public UnkiteableWeatherReconProcessor(String kvStoreNameUnkiteable, String kvStoreNameKiteable, String location) {
		this.kvStoreNameUnkiteable = kvStoreNameUnkiteable;
		this.kvStoreNameKiteable = kvStoreNameKiteable;
		this.location = location;
		this.unkiteableKeyName = String.format("%sUnkiteable", location);
		this.kiteableKeyName = String.format("%sKiteable", location);
	}
	
	@Override
	public void init(ProcessorContext<String, NoKiteableWeatherDetected> context) {
		Processor.super.init(context);
		
		this.context = context;
		stateStoreUnkiteable = context.getStateStore(this.kvStoreNameUnkiteable);
		stateStoreKiteable = context.getStateStore(this.kvStoreNameKiteable);
	}
	
	@Override
	public void process(Record<String, GenericRecord> record) {
		
		NoKiteableWeatherDetected mostRecentUnkiteableEvent = stateStoreUnkiteable.get(unkiteableKeyName);
		KiteableWeatherDetected mostRecentKiteableEvent = stateStoreKiteable.get(kiteableKeyName);
		
		if(mostRecentUnkiteableEvent == null) {
			mostRecentUnkiteableEvent = NoKiteableWeatherDetected.newBuilder()
					.setDataID(unkiteableKeyName)
					.setLocatie(location)
					.build();
		}
		
		if(mostRecentKiteableEvent == null) {
			mostRecentKiteableEvent = KiteableWeatherDetected.newBuilder()
					.setDataID(kiteableKeyName)
					.setLocatie(location)
					.build();
			
		}
		
		String schemaName = record.value().getSchema().getName();
		
		if(schemaName.equals("UnkiteableWindDetected")) {
			mostRecentUnkiteableEvent.setWindsnelheid(record.value().get("Waarde").toString());
			mostRecentUnkiteableEvent.setEenheidWindsnelheid(record.value().get("Eenheid").toString());
			
			mostRecentKiteableEvent.setWindsnelheid("");
			mostRecentKiteableEvent.setEenheidWindsnelheid("");
		}
		
		if(schemaName.equals("UnkiteableWaveDetected")) {
			mostRecentUnkiteableEvent.setGolfhoogte(record.value().get("Waarde").toString());
			mostRecentUnkiteableEvent.setEenheidGolfhoogte(record.value().get("Eenheid").toString());
			
			mostRecentKiteableEvent.setGolfhoogte("");
			mostRecentKiteableEvent.setEenheidGolfhoogte("");
		}
		
		if(schemaName.equals("UnkiteableWindDirectionDetected")) {
			mostRecentUnkiteableEvent.setWindrichting(record.value().get("Waarde").toString());
			mostRecentUnkiteableEvent.setEenheidWindrichting(record.value().get("Eenheid").toString());
			
			mostRecentKiteableEvent.setWindrichting("");
			mostRecentKiteableEvent.setEenheidWindrichting("");
		}
		
		mostRecentUnkiteableEvent.setGolfhoogte(record.value().get("Waarde").toString());
		mostRecentUnkiteableEvent.setEenheidGolfhoogte(record.value().get("Eenheid").toString());
		
		mostRecentKiteableEvent.setGolfhoogte("");
		mostRecentKiteableEvent.setEenheidGolfhoogte("");
		
		if(mostRecentUnkiteableEvent.getTijdstip() < (long) record.value().get("Tijdstip")) {
			mostRecentUnkiteableEvent.setTijdstip((long) record.value().get("Tijdstip"));
		}
		
		stateStoreUnkiteable.put(unkiteableKeyName, mostRecentUnkiteableEvent);
    	
    	if(thereAreKiteableCircumstances(mostRecentUnkiteableEvent)) {
    		return;
    	}
        
        var output = new Record<>(unkiteableKeyName, mostRecentUnkiteableEvent, record.timestamp(), record.headers());
        context.forward(output);
        
		return;
	}
	
	@Override
	public void close() {
		
	}
	
	private static boolean thereAreKiteableCircumstances(NoKiteableWeatherDetected event) {
		return event.getWindsnelheid().isEmpty() || event.getGolfhoogte().isEmpty() || event.getWindrichting().isEmpty();
	}
}
