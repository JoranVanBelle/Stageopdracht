package com.stage.adapter.mvb.processors;


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KiteableWindDetected;
import com.stage.RawDataMeasured;
import com.stage.WindHasFallenOff;

public class KiteableWindSpeedProcessor implements Processor<String, RawDataMeasured, String, RawDataMeasured> {

	private KeyValueStore<String, RawDataMeasured> stateStore;
	private ProcessorContext<String, RawDataMeasured> context;
	
	private String kvStoreName;
	private double threshold;

	public KiteableWindSpeedProcessor(String kvStoreName, double threshold){
	    this.kvStoreName = kvStoreName;
	    this.threshold = threshold;
	}
	
	@Override
    public void init(ProcessorContext<String, RawDataMeasured> context) {
		// TODO Auto-generated method stub
		Processor.super.init(context);
		
		this.context = context;
		stateStore = context.getStateStore(this.kvStoreName);
	}
	
	@Override
	public void process(Record<String, RawDataMeasured> record) {
		// TODO Auto-generated method stub		
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
        
        if(isRecordEersteRecordInStateStore(mostRecentEvent, record)) {
            RawDataMeasured kitableWindDetected = new RawDataMeasured(
            		record.value().getSensorID(),
            		record.value().getLocatie(),
            		record.value().getWaarde(),
            		record.value().getEenheid(),
            		record.value().getTijdstip()
            		);
            
            var output = new Record<>(record.key(), kitableWindDetected, record.timestamp(), record.headers());
            context.forward(output);
        }
        
        if (isValueOverThresholdAndIsLastValueOverThreshold(mostRecentEvent, record, threshold)) {
        	return;
        	
        } else if (isValueOverThresholdAndIsLastValueLessThanThreshold(mostRecentEvent, record, threshold)) {
        	stateStore.put(record.key(), record.value());
        	
            RawDataMeasured kitableWindDetected = new RawDataMeasured(
            		record.value().getSensorID(),
            		record.value().getLocatie(),
            		record.value().getWaarde(),
            		record.value().getEenheid(),
            		record.value().getTijdstip()
            		);
            
            var output = new Record<>(record.key(), kitableWindDetected, record.timestamp(), record.headers());
            context.forward(output);
            return;
            
        } else if(isValueLessThanThresholdAndIsLastOverThreshold(mostRecentEvent, record, threshold)) {
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
		// TODO Auto-generated method stub
    }
    
    private static boolean isRecordEersteRecordInStateStore(RawDataMeasured mostRecentEvent, Record<String, RawDataMeasured> record) {
    	return mostRecentEvent.getTijdstip() == record.value().getTijdstip();
    }
	
	private static boolean isValueOverThresholdAndIsLastValueOverThreshold(RawDataMeasured mostRecentEvent, Record<String, RawDataMeasured> record, double threshold) {
		return Double.parseDouble(record.value().getWaarde()) > threshold && Double.parseDouble(mostRecentEvent.getWaarde()) > threshold;
	}
	
	private static boolean isValueOverThresholdAndIsLastValueLessThanThreshold(RawDataMeasured mostRecentEvent,  Record<String, RawDataMeasured> record, double threshold) {
		return Double.parseDouble(record.value().getWaarde()) > threshold && Double.parseDouble(mostRecentEvent.getWaarde()) < threshold;
	}	
	
	private static boolean isValueLessThanThresholdAndIsLastOverThreshold(RawDataMeasured mostRecentEvent,  Record<String, RawDataMeasured> record, double threshold) {
		return Double.parseDouble(record.value().getWaarde()) < threshold && Double.parseDouble(mostRecentEvent.getWaarde()) > threshold;
	}

}
