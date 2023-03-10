package com.stage.adapter.mvb.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KiteableCircumstancesDetected;
import com.stage.KiteableWindDetected;

public class ReconcilationProcessor implements Processor<String, KiteableWindDetected, String, KiteableCircumstancesDetected> {

	private KeyValueStore<String, KiteableCircumstancesDetected> store;
	private ProcessorContext<String, KiteableCircumstancesDetected> context;
	
	@Override
	public void init(ProcessorContext<String, KiteableCircumstancesDetected> context) {
		Processor.super.init(context);
		
		this.context = context;
		store = context.getStateStore("merge_store");
	}
	
	@Override
	public void process(Record<String, KiteableWindDetected> record) {
		// TODO Auto-generated method stub
		KiteableCircumstancesDetected storedValue = store.get(record.key());
		
	}
	
    @Override
    public void close() {

    }

}
