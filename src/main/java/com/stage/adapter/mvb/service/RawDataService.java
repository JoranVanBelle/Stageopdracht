package com.stage.adapter.mvb.service;

import java.util.Properties;

import com.stage.RawDataMeasured;
import com.stage.adapter.mvb.infrastructure.RawDataInfrastructure;

public class RawDataService {
	
	private final RawDataInfrastructure rawDataInfrastructure;
	
	public RawDataService(Properties props) {
		rawDataInfrastructure = new RawDataInfrastructure(props);
	}
	
	//testpurposes
	public RawDataService(RawDataInfrastructure rawDataInfrastructure) {
		this.rawDataInfrastructure = rawDataInfrastructure;
	}
	
	public void produce(RawDataMeasured rawDataMeasured) {
		rawDataInfrastructure.produce(rawDataMeasured);
	}
	
}
