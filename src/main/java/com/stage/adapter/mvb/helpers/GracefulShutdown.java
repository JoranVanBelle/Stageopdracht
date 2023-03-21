package com.stage.adapter.mvb.helpers;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.streams.KafkaStreams;

public class GracefulShutdown {

    public static void gracefulShutdown(KafkaStreams stream) {
    	final CountDownLatch latch = new CountDownLatch(1);

    	Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
    	    @Override
    	    public void run() {
    	        stream.close();
    	        latch.countDown();
    	    }
    	});

    	try {
    	    stream.start();
    	    latch.await();
    	} catch (Throwable e) {
    	    System.exit(1);
    	}
    	System.exit(0);
}
	
}
