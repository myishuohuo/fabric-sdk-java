package org.hyperledger.fabric.sdk.ManageChannelPool;

import io.grpc.ManagedChannel;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author oxchains.huohuo
 * */
public class ConnectionMaxAgeThread implements Runnable {
	/** Max no of ms to wait before a connection that isn't used is killed off. */
	private long maxAgeInMs;
	/** Partition being handled. */
	private ConnectionPartition partition;
	/** Scheduler handle. **/
	private ScheduledExecutorService scheduler;
	/** Handle to connection pool. */
	private MChannelPool pool;
	private int poolAvailabilityThreshold;
	/** Logger handle. */
	private static final Logger logger = LoggerFactory.getLogger(ConnectionTesterThread.class);

	protected ConnectionMaxAgeThread(ConnectionPartition connectionPartition, ScheduledExecutorService scheduler,
			MChannelPool pool, long maxAgeInMs) {
		this.partition = connectionPartition;
		this.scheduler = scheduler;
		this.maxAgeInMs = maxAgeInMs;
		this.pool = pool;
	}
	/** Invoked periodically. */
	public void run() {
		ManagedChannelHandle handle = null;
		long tmp;
		long nextCheckInMs = this.maxAgeInMs;
		int partitionSize = this.partition.getAvailableConnections();
		long currentTime = System.currentTimeMillis();
		for (int i = 0; i < partitionSize; i++) {
			try {
				handle = this.partition.getFreeConnections().poll();
				if(null != handle){
	                if(checkChannel(handle.getManagedChannel())){
	                	tmp = nextCheckInMs - (currentTime - handle.getConnectionCreationTimeInMs());
	                	if (tmp < nextCheckInMs){
							nextCheckInMs = tmp; 
						}
	                	if (currentTime - handle.getConnectionCreationTimeInMs() > this.maxAgeInMs){
	                		handle.resetManagedChannel();
						}
	                	this.partition.putConnectionBackInPartition(handle);
	                }
                
				}
				else{
					break;
				}
				Thread.sleep(20L); // test slowly, this is not an operation that we're in a hurry to deal with (avoid CPU spikes)...
			} catch (Throwable e) {
				if (this.scheduler.isShutdown()) {
					logger.debug("Shutting down connection max age thread.");
				} else {
					logger.error("Connection max age thread exception.", e);
				}
			}
		}
		if (!this.scheduler.isShutdown()) {
			this.scheduler.schedule(this, nextCheckInMs, TimeUnit.MILLISECONDS);
		}
	}
	public static boolean checkChannel(ManagedChannel channel){
		if(null != channel && !channel.isShutdown() && !channel.isTerminated()){
			return true;
		}
		return false;
	}
	
}
