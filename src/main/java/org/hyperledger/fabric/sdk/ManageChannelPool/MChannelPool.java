package org.hyperledger.fabric.sdk.ManageChannelPool;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.util.concurrent.MoreExecutors;
 /**
  * @author oxchains.huohuo
  * */

public class MChannelPool implements Serializable {
	private static final long serialVersionUID = -6981561271612372786L;
	private static final Logger logger = LoggerFactory.getLogger(MChannelPool.class);
	/** the CNManagedChannel is and not shutdoen */
	protected volatile boolean poolShuttingDown;
	/** The data structure of the connection pool */
	private static Map<String, ConnectionPartition> map;
	/**
	 * Handle to factory that creates 1 thread per partition that periodically
	 * wakes up and performs some activity on the connection.
	 */
	protected ScheduledExecutorService keepAliveScheduler;
	/**
	 * Handle to factory that creates 1 thread per partition that periodically
	 * wakes up and performs some activity on the connection.
	 */
	private ScheduledExecutorService maxAliveScheduler;
	/**
	 * Handle to factory that creates 1 thread per partition that periodically
	 * wakes up and performs some activity on the connection.
	 */
	private ExecutorService connectionsScheduler;
	/**
	 * Handle to factory that creates 1 thread per partition that periodically
	 * wakes up and performs some activity on the connection.
	 */
	private ExecutorService asyncExecutor;
	/** Configuration object used in constructor. */
	private ManagedChannelConfig config;
	/**
	 * Time to wait before timing out the connection. Default in config is
	 * Long.MAX_VALUE milliseconds.
	 */
	protected long connectionTimeoutInMs;
	/** the partition count */
	private int partitionCount;

	/**
	 * Closes off this connection pool.
	 */
	public synchronized void shutdown() {
		if (!this.poolShuttingDown) {
			logger.info("Shutting down connection pool...");
			this.poolShuttingDown = true;
			this.keepAliveScheduler.shutdownNow(); // stop threads from firing.
			this.maxAliveScheduler.shutdownNow(); // stop threads from firing.
			this.connectionsScheduler.shutdownNow(); // stop threads from
														// firing.
			this.asyncExecutor.shutdownNow();
			try {
				this.connectionsScheduler.awaitTermination(5L, TimeUnit.SECONDS);
				this.maxAliveScheduler.awaitTermination(5L, TimeUnit.SECONDS);
				this.keepAliveScheduler.awaitTermination(5L, TimeUnit.SECONDS);
				this.asyncExecutor.awaitTermination(5L, TimeUnit.SECONDS);
				this.terminateAllConnections();
			} catch (InterruptedException localInterruptedException) {
			}
			logger.info("Connection pool has been shutdown.");
		}
	}

	/** Just a synonym to shutdown. */
	public void close() {
		shutdown();
	}

	/**
	 * Constructor.
	 *
	 * 
	 */
	public MChannelPool() {
		try {
			this.config = new ManagedChannelConfig();
		this.config.sanitize();

		this.asyncExecutor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
		this.keepAliveScheduler = Executors.newScheduledThreadPool(this.config.getPartitionCount(),new CustomThreadFactory("keep-alive-scheduler", true));
		this.maxAliveScheduler = Executors.newScheduledThreadPool(this.config.getPartitionCount(),new CustomThreadFactory("max-alive-scheduler", true));
		this.connectionsScheduler = Executors.newFixedThreadPool(this.config.getPartitionCount(),new CustomThreadFactory("pool-watch-thread", true));
		this.partitionCount = this.config.getPartitionCount();

		initPartition();
		} 
		catch (Exception e) {
			logger.error("init MChannelPool failure", e);
		}
	}

	/**
	 * Releases the given connection back to the pool. This method is not
	 * intended to be called by applications (hence set to protected). Call
	 * connection.close() instead which will return the connection back to the
	 * pool.
	 */
	public void releaseManagedChannelHandle(ManagedChannelHandle handle,String addr, String port) {
		try {
			ManagedChannelHandle handles = handle;
			if ((!this.poolShuttingDown) && (handles != null) && (!handles.isShutdown())
					&& (!handles.isTerminated())) {
				handles.setConnectionLastUsedInMs(System.currentTimeMillis());
				ConnectionPartition partition = (ConnectionPartition) map.get(addr+":"+port);
				partition.putConnectionBackInPartition(handles);
			}
		} catch (Exception e) {
			logger.error(e.toString());
		}
	}

	/** get a free Connection */
	public ManagedChannelHandle getConnection(String addr, String port){
		ManagedChannelHandle handle = null;
		try {
			handle = ((ConnectionPartition) map.get(addr+":"+port)).getManagedChannelHandle();
		} catch (Exception e) {
			logger.error("get connection failure:",e);
		}
		return handle;
	}

	/** init partition */
	public void initPartition() {
		try {
			map = new ConcurrentHashMap<String, ConnectionPartition>(this.config.getPartitionCount());
			for (int i = 0; i < this.config.getPartitionCount(); i++) {
				ConnectionPartition connectionPartition = new ConnectionPartition(this, i);
				BlockingQueue<ManagedChannelHandle> quene = new LinkedBlockingQueue<ManagedChannelHandle>(
						this.config.getMaxConnectionsPerPartition());
				connectionPartition.setFreeConnections(quene);
				String addrAndPort = this.config.getPartitionAddrAndPort()[i];
				String[] split = addrAndPort.split(":");
					for (int j = 0; j < this.config.getMinConnectionsPerPartition(); j++) {
						connectionPartition.addFreeConnection(new ManagedChannelHandle(split[0],Integer.parseInt(split[1])));
					}
					map.put(connectionPartition.getPartitionAddrAndPort(), connectionPartition);
					if ((this.config.getIdleConnectionTestPeriod(TimeUnit.SECONDS) > 0L)
							|| (this.config.getIdleMaxAge(TimeUnit.SECONDS) > 0L)) {
						Runnable connectionTester = new ConnectionTesterThread(connectionPartition, this.keepAliveScheduler,
								this, this.config.getIdleMaxAge(TimeUnit.MILLISECONDS),
								this.config.getIdleConnectionTestPeriod(TimeUnit.MILLISECONDS));
						long delayInSeconds = this.config.getIdleConnectionTestPeriod(TimeUnit.SECONDS);
						if (delayInSeconds == 0L) {
							delayInSeconds = this.config.getIdleMaxAge(TimeUnit.SECONDS);
						}
						if ((this.config.getIdleMaxAge(TimeUnit.SECONDS) < delayInSeconds)
								&& (this.config.getIdleConnectionTestPeriod(TimeUnit.SECONDS) != 0L)
								&& (this.config.getIdleMaxAge(TimeUnit.SECONDS) != 0L)) {
							delayInSeconds = this.config.getIdleMaxAge(TimeUnit.SECONDS);
						}
						this.keepAliveScheduler.schedule(connectionTester, delayInSeconds, TimeUnit.SECONDS);
					}
					if (this.config.getMaxConnectionAgeInSeconds() > 0L) {
						Runnable connectionMaxAgeTester = new ConnectionMaxAgeThread(connectionPartition,
								this.maxAliveScheduler, this, this.config.getMaxConnectionAge(TimeUnit.MILLISECONDS));
						this.maxAliveScheduler.schedule(connectionMaxAgeTester, this.config.getMaxConnectionAgeInSeconds(),
								TimeUnit.SECONDS);
					}
					Runnable connec= new PoolWatchThread(connectionPartition, this);
					this.connectionsScheduler.execute(connec);
			}
		} catch (Exception e) {
			logger.debug(e.toString());
		}
	}

	/**
	 * Throw an exception to capture it so as to be able to print it out later
	 * on
	 * 
	 * @param message
	 *            message to display
	 * @return Stack trace message
	 *
	 */
	protected String captureStackTrace(String message) {
		StringBuilder stringBuilder = null;
		try {
			stringBuilder = new StringBuilder(
					String.format(message, new Object[] { Thread.currentThread().getName() }));
			StackTraceElement[] trace = Thread.currentThread().getStackTrace();
			for (int i = 0; i < trace.length; i++) {
				stringBuilder.append(" " + trace[i] + "\r\n");
			}
			stringBuilder.append("");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.debug(e.toString());
		}

		return stringBuilder.toString();
	}

	public ManagedChannelConfig getConfig() {
		return this.config;
	}

	/**
	 * Return total number of connections created in all partitions.
	 *
	 * @return number of created connections
	 */
	public int getTotalCreatedConnections() throws Exception {
		int total = 0;
		try {
			Collection<ConnectionPartition> values = map.values();
			total = 0;
			for (ConnectionPartition connectionPartition : values) {
				total += connectionPartition.getCreatedConnections();
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.debug(e.toString());
		}
		return total;
	}
	public void terminateAllConnections(){
		try {
			Set<String> set = map.keySet();
			Iterator<String> iterator = set.iterator();
			while (iterator.hasNext()) {
				 String next = iterator.next();
				 map.get(next).shutdown();
			}
		} catch (Exception e) {
	
			logger.debug(e.toString());
		}
	};
}
