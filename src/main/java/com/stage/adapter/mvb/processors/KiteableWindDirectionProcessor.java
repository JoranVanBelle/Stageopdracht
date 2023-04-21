package com.stage.adapter.mvb.processors;

import java.util.Map;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.RawDataMeasured;

public class KiteableWindDirectionProcessor implements Processor<String, RawDataMeasured, String, RawDataMeasured> {

	private KeyValueStore<String, RawDataMeasured> stateStore;
	private ProcessorContext<String, RawDataMeasured> context;
	
	private String kvStoreName;
	private Map<String, double[]> thresholds;
	
	public KiteableWindDirectionProcessor(String kvStoreName, Map<String, double[]> thresholds) {
		this.kvStoreName = kvStoreName;
		this.thresholds = thresholds;
	}

	@Override
	public void init(ProcessorContext<String, RawDataMeasured> context) {
		Processor.super.init(context);
		
		this.context = context;
		stateStore = context.getStateStore(this.kvStoreName);
	}
	
	@Override
	public void process(Record<String, RawDataMeasured> record) {
		double lowerBound = thresholds.get(record.key())[0];
		double upperBound = thresholds.get(record.key())[1];
		
		var mostRecentEvent = stateStore.get(record.key());
		
		if(mostRecentEvent == null) {
        	stateStore.put(record.key(), record.value());
        	
        	RawDataMeasured firstValue = new RawDataMeasured(
        			record.value().getSensorID(),
        			record.value().getLocatie(),
        			record.value().getWaarde(),
        			record.value().getEenheid(),
        			record.value().getTijdstip()
        			);
        	
        	var output = new Record<>(record.key(), firstValue, record.timestamp(), record.headers());
        	context.forward(output);
        	return;
		}

		if(isRecordFirstRecordInStateStore(mostRecentEvent, record)) {
			return;
		}
		
		if(isValueBetweenUpperAndLowerBoundAndLastNotBetweenUpperAndLower(mostRecentEvent, record, upperBound, lowerBound)) {
        	stateStore.put(record.key(), record.value());

        	RawDataMeasured windHasFallenOff = new RawDataMeasured(
            		record.value().getSensorID(),
            		record.value().getLocatie(),
            		record.value().getWaarde(),
            		record.value().getEenheid(),
            		record.value().getTijdstip()
            		);
        	
        	var output = new Record<>(record.key(), windHasFallenOff, record.timestamp(), record.headers());
        	context.forward(output);
        	return;
		} else if(isValueNotBetweenUpperAndLowerBoundAndLastBetweenUpperAndLower(mostRecentEvent, record, upperBound, lowerBound)) {
        	stateStore.put(record.key(), record.value());

        	RawDataMeasured windHasFallenOff = new RawDataMeasured(
            		record.value().getSensorID(),
            		record.value().getLocatie(),
            		record.value().getWaarde(),
            		record.value().getEenheid(),
            		record.value().getTijdstip()
            		);
        	
        	var output = new Record<>(record.key(), windHasFallenOff, record.timestamp(), record.headers());
        	context.forward(output);
        	return;
		} else {
			return;
		}
		
	}
	
    @Override
    public void close() {

    }

	private static boolean isRecordFirstRecordInStateStore(RawDataMeasured mostRecentEvent, Record<String, RawDataMeasured> record) {
		return mostRecentEvent.getTijdstip() == record.value().getTijdstip();
	}

    private static boolean isValueBetweenUpperAndLowerBoundAndLastNotBetweenUpperAndLower(RawDataMeasured mostRecentEvent, Record<String, RawDataMeasured> record, double upper, double lower) {
    	double lastValue = Double.parseDouble(mostRecentEvent.getWaarde());
    	double newValue = Double.parseDouble(record.value().getWaarde());
    	
    	return (newValue >= lower && newValue <= upper) && (lower > lastValue || lastValue > upper);
    }
    
    private static boolean isValueNotBetweenUpperAndLowerBoundAndLastBetweenUpperAndLower(RawDataMeasured mostRecentEvent, Record<String, RawDataMeasured> record, double upper, double lower) {
    	double lastValue = Double.parseDouble(mostRecentEvent.getWaarde());
    	double newValue = Double.parseDouble(record.value().getWaarde());
    	
    	return (newValue < lower || newValue > upper) && (lastValue >= lower && lastValue <= upper);
    }
}
