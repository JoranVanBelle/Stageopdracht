package com.stage.adapter.mvb.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KitableCircumstancesDetected;
import com.stage.KitableWindDetected;

public class ReconcilationProcessor implements Processor<String, KitableWindDetected, String, KitableCircumstancesDetected> {

	private KeyValueStore<String, KitableCircumstancesDetected> store;
	private ProcessorContext<String, KitableCircumstancesDetected> context;
	
	@Override
	public void init(ProcessorContext<String, KitableCircumstancesDetected> context) {
		Processor.super.init(context);
		
		this.context = context;
		store = context.getStateStore("merge_store");
	}
	
	@Override
	public void process(Record<String, KitableWindDetected> record) {
		// TODO Auto-generated method stub
		KitableCircumstancesDetected storedValue = store.get(record.key());
		
	}
	
    @Override
    public void close() {

    }

}
