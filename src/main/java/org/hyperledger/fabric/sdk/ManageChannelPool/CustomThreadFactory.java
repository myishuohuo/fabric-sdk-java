package org.hyperledger.fabric.sdk.ManageChannelPool;

import java.util.concurrent.ThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author oxchains.huohuo
 * */
public class CustomThreadFactory implements ThreadFactory, Thread.UncaughtExceptionHandler {
	private boolean daemon;
	private String threadName;
	private static final Logger logger = LoggerFactory.getLogger(CustomThreadFactory.class);

	public CustomThreadFactory(String threadName, boolean daemon) {
		this.threadName = threadName;
		this.daemon = daemon;
	}

	public Thread newThread(Runnable r) {
		Thread t = new Thread(r, this.threadName);
		t.setDaemon(this.daemon);
		t.setUncaughtExceptionHandler(this);
		return t;
	}

	public void uncaughtException(Thread thread, Throwable throwable) {
		logger.error("Uncaught Exception in thread " + thread.getName(), throwable);
	}
}
