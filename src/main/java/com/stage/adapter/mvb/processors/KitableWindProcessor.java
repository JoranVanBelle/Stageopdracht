package com.stage.adapter.mvb.processors;


import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KitableWindDetected;
import com.stage.RawDataMeasured;

public class KitableWindProcessor implements Processor<String, RawDataMeasured, String, KitableWindDetected> {

	private KeyValueStore<String, RawDataMeasured> stateStore;
	private ProcessorContext<String, KitableWindDetected> context;
	
	@Override
    public void init(ProcessorContext<String, KitableWindDetected> context) {
		// TODO Auto-generated method stub
		Processor.super.init(context);
		
		this.context = context;
		stateStore = context.getStateStore("most-recent-event");
	}
	
	@Override
	public void process(Record<String, RawDataMeasured> record) {
		// TODO Auto-generated method stub
        var mostRecentEvent = stateStore.get(record.key());
        System.err.println("boo");
        if(mostRecentEvent == null) {
        	stateStore.put(record.key(), record.value());
        	return;
        }
        
        if (Double.parseDouble(mostRecentEvent.getWaarde()) > 7.717 == (Double.parseDouble(record.value().getWaarde()) > 7.717)) {
            return;
        } else {
        	stateStore.put(record.key(), record.value());
            KitableWindDetected kitableWind = new KitableWindDetected(
            		record.value().getSensorID(), 
            		record.value().getLocatie(), 
            		Double.parseDouble(mostRecentEvent.getWaarde()) > 7.717, 
            		record.value().getTijdstip()
            );
            
            var output = new Record<>(record.key(), kitableWind, record.timestamp(), record.headers());
            context.forward(output);
        }
	}
	
    @Override
    public void close() {
		// TODO Auto-generated method stub

    }
	
	

}
