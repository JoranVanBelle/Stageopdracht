package com.stage.adapter.mvb.processors;

import java.util.ArrayList;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KiteableWaveDetected;
import com.stage.KiteableWeatherDetected;
import com.stage.KiteableWindDetected;
import com.stage.KiteableWindDirectionDetected;
import com.stage.NoKiteableWeatherDetected;
import com.stage.UnkiteableWaveDetected;
import com.stage.UnkiteableWindDetected;
import com.stage.UnkiteableWindDirectionDetected;

public class KiteableWeatherReconProcessor implements Processor<String, GenericRecord, String, GenericRecord> {
	
	private KeyValueStore<String, KiteableWeatherDetected> stateStoreKiteable;
	private KeyValueStore<String, NoKiteableWeatherDetected> stateStoreUnkiteable;
	private ProcessorContext<String, GenericRecord> context;
	
	private String kvStoreNameKiteable;
	private String kvStoreNameUnkiteable;
	private String location;
	private String kiteableKeyName;
	private String unkiteableKeyName;
	
	
	public KiteableWeatherReconProcessor(String kvStoreNameKiteable, String kvStoreNameUnkiteabe, String location) {
		this.kvStoreNameKiteable = kvStoreNameKiteable;
		this.kvStoreNameUnkiteable = kvStoreNameUnkiteabe;
		this.location = location;
		this.kiteableKeyName = String.format("%sKiteable", location);
		this.unkiteableKeyName = String.format("%sUnkiteable", location);
	}
	
	@Override
	public void init(ProcessorContext<String, GenericRecord> context) {
		Processor.super.init(context);
		
		this.context = context;
		stateStoreKiteable = context.getStateStore(this.kvStoreNameKiteable);
		stateStoreUnkiteable = context.getStateStore(this.kvStoreNameUnkiteable);
	}
	
	@Override
	public void process(Record<String, GenericRecord> record) {
		
		KiteableWeatherDetected mostRecentKiteableEvent = stateStoreKiteable.get(kiteableKeyName);
		NoKiteableWeatherDetected mostRecentUnkiteableEvent = stateStoreUnkiteable.get(unkiteableKeyName);
		
		if(mostRecentKiteableEvent == null) {
			mostRecentKiteableEvent = KiteableWeatherDetected.newBuilder()
					.setDataID(kiteableKeyName)
					.setLocatie(location)
					.build();
		}
		
		
		if(mostRecentUnkiteableEvent == null) {
			mostRecentUnkiteableEvent = NoKiteableWeatherDetected.newBuilder()
					.setDataID(unkiteableKeyName)
					.setLocatie(location)
					.build();
		}
		
		if(schemaStartsWithKiteable(record)) {
			var kiteable = updateKiteableWeatherEventsAndReturnUpdatedEvent(record, mostRecentKiteableEvent, mostRecentUnkiteableEvent);
			var unkiteable = enterValuesInUnkiteable(record, mostRecentUnkiteableEvent);
			
			stateStoreKiteable.put(kiteableKeyName, kiteable);
			stateStoreUnkiteable.put(unkiteableKeyName, unkiteable);
			
			if(allKiteableFieldAreFilledIn(kiteable)) {
				var output = new Record<>(String.format("%s%d", kiteableKeyName, kiteable.getTijdstip()), kiteable, record.timestamp(), record.headers());
				context.forward(output);
				return;
			} else if(allUnkiteableFieldAreFilledIn(unkiteable)) {
				var output = new Record<>(String.format("%s%d", unkiteableKeyName, unkiteable.getTijdstip()), unkiteable, record.timestamp(), record.headers());
				context.forward(output);
				return;
			}
			
		} else {
			var list = updateUnkiteableWeatherEventsAndReturnUpdatedEvent(record, mostRecentKiteableEvent, mostRecentUnkiteableEvent);
			
			var unkiteable = (NoKiteableWeatherDetected) list.get(0);
			var kiteable = (KiteableWeatherDetected) list.get(1);
			
			stateStoreUnkiteable.put(unkiteableKeyName, unkiteable);
			stateStoreKiteable.put(kiteableKeyName, kiteable);
			
			if(allUnkiteableFieldAreFilledIn(unkiteable)) {
				var output = new Record<>(String.format("%s%d", unkiteableKeyName, unkiteable.getTijdstip()), unkiteable, record.timestamp(), record.headers());
				context.forward(output);
			}
			
		}		
	}
	
	@Override
	public void close() {
		
	}
	
	
	
	private static boolean allKiteableFieldAreFilledIn(KiteableWeatherDetected event) {
		return !event.getWindsnelheid().equals("") && !event.getGolfhoogte().equals("") && !event.getWindrichting().equals("");
	}
	
	private static boolean allUnkiteableFieldAreFilledIn(NoKiteableWeatherDetected event) {
		return !event.getWindsnelheid().equals("") && !event.getGolfhoogte().equals("") && !event.getWindrichting().equals("");
	}
	
	private static boolean schemaStartsWithKiteable(Record<String, GenericRecord> record) {
		return record.value().getSchema().getName().toString().startsWith("Kiteable");
	}
	
	private static List<Object> updateUnkiteableWeatherEventsAndReturnUpdatedEvent(
			Record<String, GenericRecord> record,
			KiteableWeatherDetected mostRecentKiteableEvent, 
			NoKiteableWeatherDetected mostRecentUnkiteableEvent
	) {
		String schemaName = record.value().getSchema().getName();
		
		if(schemaName.equals("UnkiteableWindDetected")) {

			UnkiteableWindDetected transformed = (UnkiteableWindDetected) SpecificData.get().deepCopy(record.value().getSchema(), record.value());
			
			mostRecentUnkiteableEvent.setWindsnelheid(transformed.getWaarde());
			mostRecentUnkiteableEvent.setEenheidWindsnelheid(transformed.getEenheid());
			
			mostRecentKiteableEvent.setWindsnelheid("");
			mostRecentKiteableEvent.setEenheidWindsnelheid("");
			
//			mostRecentUnkiteableEvent.setGolfhoogte(mostRecentKiteableEvent.getGolfhoogte());
//			mostRecentUnkiteableEvent.setEenheidGolfhoogte(mostRecentKiteableEvent.getEenheidGolfhoogte());
//			
//			mostRecentUnkiteableEvent.setWindrichting(mostRecentKiteableEvent.getWindrichting());
//			mostRecentUnkiteableEvent.setEenheidWindrichting(mostRecentKiteableEvent.getEenheidWindrichting());

			if(mostRecentUnkiteableEvent.getTijdstip() < transformed.getTijdstip()) {
				mostRecentUnkiteableEvent.setTijdstip(transformed.getTijdstip());
			}
		}
		
		if(schemaName.equals("UnkiteableWaveDetected")) {
			
			UnkiteableWaveDetected transformed = (UnkiteableWaveDetected) SpecificData.get().deepCopy(record.value().getSchema(), record.value());
			
//			mostRecentUnkiteableEvent.setWindsnelheid(mostRecentKiteableEvent.getWindsnelheid());
//			mostRecentUnkiteableEvent.setEenheidWindsnelheid(mostRecentKiteableEvent.getEenheidWindsnelheid());
			
			mostRecentUnkiteableEvent.setGolfhoogte(transformed.getWaarde());
			mostRecentUnkiteableEvent.setEenheidGolfhoogte(transformed.getEenheid());
			
			mostRecentKiteableEvent.setGolfhoogte("");
			mostRecentKiteableEvent.setEenheidGolfhoogte("");
			
//			mostRecentUnkiteableEvent.setWindrichting(mostRecentKiteableEvent.getWindrichting());
//			mostRecentUnkiteableEvent.setEenheidWindrichting(mostRecentKiteableEvent.getEenheidWindrichting());

			if(mostRecentUnkiteableEvent.getTijdstip() < transformed.getTijdstip()) {
				mostRecentUnkiteableEvent.setTijdstip(transformed.getTijdstip());
			}
		}
		
		if(schemaName.equals("UnkiteableWindDirectionDetected")) {
			
			UnkiteableWindDirectionDetected transformed = (UnkiteableWindDirectionDetected) SpecificData.get().deepCopy(record.value().getSchema(), record.value());
			
			mostRecentUnkiteableEvent.setWindsnelheid(mostRecentKiteableEvent.getWindsnelheid());
			mostRecentUnkiteableEvent.setEenheidWindsnelheid(mostRecentKiteableEvent.getEenheidWindsnelheid());
			
			mostRecentUnkiteableEvent.setGolfhoogte(mostRecentKiteableEvent.getGolfhoogte());
			mostRecentUnkiteableEvent.setEenheidGolfhoogte(mostRecentKiteableEvent.getEenheidGolfhoogte());
			
			mostRecentUnkiteableEvent.setWindrichting(transformed.getWaarde());
			mostRecentUnkiteableEvent.setEenheidWindrichting(transformed.getWaarde());
			
//			mostRecentKiteableEvent.setWindrichting("");
//			mostRecentKiteableEvent.setEenheidWindrichting("");

			if(mostRecentUnkiteableEvent.getTijdstip() < transformed.getTijdstip()) {
				mostRecentUnkiteableEvent.setTijdstip(transformed.getTijdstip());
			}
		}
		
		List<Object> list = new ArrayList<>();
		list.add(mostRecentUnkiteableEvent);
		list.add(mostRecentKiteableEvent);
		
		return list;
		
	}
	
	private static KiteableWeatherDetected updateKiteableWeatherEventsAndReturnUpdatedEvent(
			Record<String, GenericRecord> record,
			KiteableWeatherDetected mostRecentKiteableEvent, 
			NoKiteableWeatherDetected mostRecentUnkiteableEvent
	) {
		String schemaName = record.value().getSchema().getName();
		
		if(schemaName.equals("KiteableWindDetected")) {

			KiteableWindDetected transformed = (KiteableWindDetected) SpecificData.get().deepCopy(record.value().getSchema(), record.value());
			
			mostRecentKiteableEvent.setWindsnelheid(transformed.getWaarde());
			mostRecentKiteableEvent.setEenheidWindsnelheid(transformed.getEenheid());
			
			mostRecentUnkiteableEvent.setWindsnelheid("");
			mostRecentUnkiteableEvent.setEenheidWindsnelheid("");
			
			if((Long) mostRecentKiteableEvent.getTijdstip() == null || mostRecentKiteableEvent.getTijdstip() < transformed.getTijdstip()) {
				mostRecentKiteableEvent.setTijdstip(transformed.getTijdstip());
			}
		}
		
		if(schemaName.equals("KiteableWaveDetected")) {
			
			KiteableWaveDetected transformed = (KiteableWaveDetected) SpecificData.get().deepCopy(record.value().getSchema(), record.value());
			
			mostRecentKiteableEvent.setGolfhoogte(transformed.getWaarde());
			mostRecentKiteableEvent.setEenheidGolfhoogte(transformed.getEenheid());
			
			mostRecentUnkiteableEvent.setGolfhoogte("");
			mostRecentUnkiteableEvent.setEenheidGolfhoogte("");

			if((Long) mostRecentKiteableEvent.getTijdstip() == null || mostRecentKiteableEvent.getTijdstip() < transformed.getTijdstip()) {
				mostRecentKiteableEvent.setTijdstip(transformed.getTijdstip());
			}
		}
		
		if(schemaName.equals("KiteableWindDirectionDetected")) {
			
			KiteableWindDirectionDetected transformed = (KiteableWindDirectionDetected) SpecificData.get().deepCopy(record.value().getSchema(), record.value());
			
			mostRecentKiteableEvent.setWindrichting(transformed.getWaarde());
			mostRecentKiteableEvent.setEenheidWindrichting(transformed.getEenheid());
			
			mostRecentUnkiteableEvent.setWindrichting("");
			mostRecentUnkiteableEvent.setEenheidWindrichting("");

			if((Long) mostRecentKiteableEvent.getTijdstip() == null || mostRecentKiteableEvent.getTijdstip() < transformed.getTijdstip()) {
				mostRecentKiteableEvent.setTijdstip(transformed.getTijdstip());
			}
		}
		
		return mostRecentKiteableEvent;
		
	}
	
	private static NoKiteableWeatherDetected enterValuesInUnkiteable(
			Record<String, GenericRecord> record,
			NoKiteableWeatherDetected mostRecentUnkiteableEvent
	) {
		
		String schemaName = record.value().getSchema().getName();
		
		if(schemaName.contains("WindDetected")) {
			mostRecentUnkiteableEvent.setWindsnelheid(record.value().get("Waarde").toString());
			mostRecentUnkiteableEvent.setEenheidWindsnelheid(record.value().get("Eenheid").toString());
		} else if(schemaName.contains("WaveDetected")) {
			mostRecentUnkiteableEvent.setGolfhoogte(record.value().get("Waarde").toString());
			mostRecentUnkiteableEvent.setEenheidGolfhoogte(record.value().get("Eenheid").toString());
		} else if(schemaName.contains("WindDirectionDetected")) {
			mostRecentUnkiteableEvent.setWindrichting(record.value().get("Waarde").toString());
			mostRecentUnkiteableEvent.setEenheidWindrichting(record.value().get("Eenheid").toString());
		}
		
		return mostRecentUnkiteableEvent;
	}
	
}
