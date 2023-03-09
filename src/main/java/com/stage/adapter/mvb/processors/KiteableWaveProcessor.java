package com.stage.adapter.mvb.processors;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

import com.stage.KiteableCircumstancesDetected;
import com.stage.KiteableWaveDetected;

public class KiteableWaveProcessor implements Processor<String, KiteableWaveDetected, String, KiteableCircumstancesDetected>{

	private KeyValueStore<String, KiteableCircumstancesDetected> store;
	private ProcessorContext<String, KiteableCircumstancesDetected> context;
	
	@Override
	public void init(ProcessorContext<String, KiteableCircumstancesDetected> context) {
		Processor.super.init(context);
		
		this.context = context;
		store = context.getStateStore("merge_store");
	}
	
	@Override
	public void process(Record<String, KiteableWaveDetected> record) {
		// TODO Auto-generated method stub
		
	}
	
    @Override
    public void close() {

    }

}
