package com.stage.adapter.mvb;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;

import com.stage.adapter.mvb.helpers.ApplicationHelper;
import com.stage.adapter.mvb.producers.Catalog;
import com.stage.adapter.mvb.producers.CurrentData;
import com.stage.adapter.mvb.streams.KiteableWaveStream;
import com.stage.adapter.mvb.streams.KiteableWindStream;
import com.stage.adapter.mvb.streams.KiteableWinddirectionStream;

public class Application {

	private static String API = "https://api.meetnetvlaamsebanken.be";

//	private static final String[] sensoren = {"A2BGHA", "WDLGHA", "RA2GHA", "OSNGHA", "NPBGHA", "SWIGHA",
//			"MP0WC3", "MP7WC3", "NP7WC3", "MP0WVC", "MP7WVC", "NP7WVC", "A2BRHF", "RA2RHF", "OSNRHF"};
	
	
	private static final Logger logger = LogManager.getLogger(Application.class);
	
	
	public static void main(String[] args) {
		Configurator.initialize(null, "src/main/resources/log4j2.xml");
		
		CurrentData currentData = new CurrentData(API);
		Catalog catalog = new Catalog(API);
		KiteableWindStream kitableWindStream = new KiteableWindStream();
		KiteableWaveStream kitableWaveStream = new KiteableWaveStream();
		KiteableWinddirectionStream kiteableWinddirectionStream = new KiteableWinddirectionStream();

		
		Thread currentDataThread = new Thread(currentData);
		Thread catalogThread = new Thread(catalog);
		Thread windStreamThread = new Thread(kitableWindStream);
		Thread waveStreamThread = new Thread(kitableWaveStream);
		Thread kiteableWinddirectionStreamThread = new Thread(kiteableWinddirectionStream);
		
		currentDataThread.start();
		catalogThread.start();
		
		
		while(currentData.getCurrentDataString() == null || catalog.getCatalogString() == null) {
			if(currentData.getCurrentDataString() == null) {
				logger.info("ℹ️ retrieving current data" );
			}
			if(catalog.getCatalogString() == null) {
				logger.info("ℹ️ Retrieving catalog");
			}
			
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				logger.error("❌ ", e);
			}
			
		};

		
		ApplicationHelper applicationHelper = new ApplicationHelper(currentData, catalog);
		Thread applicationHelperThread = new Thread(applicationHelper);
		
		applicationHelperThread.start();
		windStreamThread.start();
		waveStreamThread.start();
		kiteableWinddirectionStreamThread.start();
	}
}
