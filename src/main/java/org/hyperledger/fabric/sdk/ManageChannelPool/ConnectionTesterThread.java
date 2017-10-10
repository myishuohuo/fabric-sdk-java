package org.hyperledger.fabric.sdk.ManageChannelPool;

import io.grpc.ManagedChannel;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionTesterThread implements Runnable {
	/** Connections used less than this time ago are not keep-alive tested. */
	private long idleConnectionTestPeriodInMs;
	/** Max no of ms to wait before a connection that isn't used is killed off. */
	private long idleMaxAgeInMs;
	/** Partition being handled. */
	private ConnectionPartition partition;
	/** Scheduler handle. **/
	private ScheduledExecutorService scheduler;
	/** Handle to connection pool. */
	private MChannelPool pool;
	private static final Logger logger = LoggerFactory.getLogger(ConnectionTesterThread.class);

	protected ConnectionTesterThread(ConnectionPartition connectionPartition, ScheduledExecutorService scheduler,
			MChannelPool pool, long idleMaxAgeInMs, long idleConnectionTestPeriodInMs) {
		this.partition = connectionPartition;
		this.scheduler = scheduler;
		this.idleMaxAgeInMs = idleMaxAgeInMs;
		this.idleConnectionTestPeriodInMs = idleConnectionTestPeriodInMs;
		this.pool = pool;
	}

	public void run() {
		ManagedChannelHandle handle = null;
	long tmp;
		try {
			long nextCheckInMs = this.idleConnectionTestPeriodInMs;
			
			if (this.idleMaxAgeInMs > 0L) {
				if (this.idleConnectionTestPeriodInMs == 0L) {
					nextCheckInMs = this.idleMaxAgeInMs;
				} else {
					nextCheckInMs = Math.min(nextCheckInMs, this.idleMaxAgeInMs);
				}
			}
			int partitionSize = this.partition.getAvailableConnections();
			long currentTimeInMs = System.currentTimeMillis();
			for (int i = 0; i < partitionSize; i++) {
				handle = this.partition.getFreeConnections().poll();
				if(null != handle){
					if(idleMaxAgeInMs >0 && currentTimeInMs - handle.getConnectionLastUsedInMs() > idleMaxAgeInMs ){
						handle.resetManagedChannel();
					}
					if (this.idleConnectionTestPeriodInMs > 0 && (currentTimeInMs-handle.getConnectionLastUsedInMs() > this.idleConnectionTestPeriodInMs)) {
						// calculate the next time to wake up
						tmp = this.idleConnectionTestPeriodInMs;
						if (this.idleMaxAgeInMs > 0){ // wake up earlier for the idleMaxAge test?
							tmp = Math.min(tmp, this.idleMaxAgeInMs);
						}
					}
					this.partition.putConnectionBackInPartition(handle);
				}
				else{
					break;
				}
			}
			this.scheduler.schedule(this, nextCheckInMs, TimeUnit.MILLISECONDS);
		} catch (Throwable e) {
			if (this.scheduler.isShutdown()) {
				logger.debug("Shutting down connection tester thread.");
			} else {
				logger.error("Connection tester thread interrupted", e);
			}
		}
	}
}
