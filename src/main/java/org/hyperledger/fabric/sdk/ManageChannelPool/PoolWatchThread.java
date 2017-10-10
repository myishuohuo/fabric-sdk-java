package org.hyperledger.fabric.sdk.ManageChannelPool;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author oxchains.huohuo
 * */
public class PoolWatchThread implements Runnable {
	/** Partition being monitored. */
	private ConnectionPartition partition;
	/** Scheduler handle. **/
	private ScheduledExecutorService scheduler;
	/** Pool handle. */
	/** Mostly used to break out easily in unit testing. */
	private boolean signalled = true;
	private MChannelPool pool;
	/** Occupancy% threshold. */
	private int poolAvailabilityThreshold;
	/** How long to wait before retrying to add a connection upon failure. */
	private long acquireRetryDelayInMs = 1000L;
	/** Start off lazily. */
	protected boolean lazyInit;
	
	private long poolWatchTesterSeconds = 3L;
	private static final Logger logger = LoggerFactory.getLogger(PoolWatchThread.class);

	public PoolWatchThread(ConnectionPartition connectionPartition,MChannelPool pool) {
		this.partition = connectionPartition;
		this.pool = pool;
		this.lazyInit = this.pool.getConfig().isLazyInit();
		this.acquireRetryDelayInMs = this.pool.getConfig().getAcquireRetryDelayInMs();
		this.poolAvailabilityThreshold = this.pool.getConfig().getPoolAvailabilityThreshold();
	}

	public void run() {
	    int maxNewConnections;
	    while (this.signalled) {
			try {
				maxNewConnections = this.partition.getMaxConnections() - this.partition.getCreatedConnections();
				while(maxNewConnections == 0 || (this.partition.getAvailableConnections() *100/this.partition.getMaxConnections() > this.poolAvailabilityThreshold)){
					if (maxNewConnections == 0){
						this.partition.setUnableToCreateMoreTransactions(true);
					}
					this.partition.getPoolWatchThreadSignalQueue().take();
					maxNewConnections = this.partition.getMaxConnections()-this.partition.getCreatedConnections();
				}
				if (maxNewConnections > 0 
						&& !this.pool.poolShuttingDown){
					this.partition.fillConnections(Math.min(maxNewConnections, this.partition.getAcquireIncrement()));
					// for the case where we have killed off all our connections due to network/db error
					if (this.partition.getCreatedConnections() < this.partition.getMinConnections()){
					this.partition.fillConnections(this.partition.getMinConnections() - this.partition.getCreatedConnections() );
							
					}
				}
			} catch (Exception e) {
				logger.debug("Terminating pool watch thread");
				return;
			}
		}
	} 
}
