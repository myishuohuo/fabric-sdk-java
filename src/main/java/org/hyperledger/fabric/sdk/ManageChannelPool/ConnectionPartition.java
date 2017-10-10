package org.hyperledger.fabric.sdk.ManageChannelPool;
import io.grpc.ManagedChannel;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * @author oxchains.huohuo
 * */
public class ConnectionPartition implements Serializable {
	private static final long serialVersionUID = -7864443421028454573L;
	private static final Logger logger = LoggerFactory.getLogger(ConnectionPartition.class);
	private BlockingQueue<ManagedChannelHandle> freeConnections;
	private final int acquireIncrement;
	private final int minConnections;
	private final int maxConnections;
	protected ReentrantReadWriteLock statsLock = new ReentrantReadWriteLock();
	private int createdConnections = 0;
	private final String addr;
	private final String port;
	private volatile boolean unableToCreateMoreTransactions = false;
	private boolean disableTracking;
	private BlockingQueue<Object> poolWatchThreadSignalQueue = new ArrayBlockingQueue(1);
	private long queryExecuteTimeLimitInNanoSeconds;
	private String poolName;
	protected MChannelPool pool;
	private String partitionAddrAndPort;
	protected BlockingQueue<Object> getPoolWatchThreadSignalQueue() {
		return this.poolWatchThreadSignalQueue;
	}
	protected void updateCreatedConnections(int increment) {
		try {
			this.statsLock.writeLock().lock();
			this.createdConnections += increment;
			this.statsLock.writeLock().unlock();

		}catch(Exception e){
			this.statsLock.writeLock().unlock();
			logger.debug(e.toString());
		}
	}

	protected void addFreeConnection(ManagedChannelHandle managedChannel) throws SQLException {
		if (this.createdConnections < this.maxConnections && this.freeConnections.offer(managedChannel)) {
				updateCreatedConnections(1);
		}
		else{
			try {
				managedChannel.shutdown().awaitTermination(5L, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				logger.debug(e.toString());
			}
		}

	}

	protected BlockingQueue<ManagedChannelHandle> getFreeConnections() throws Exception {
		return this.freeConnections;
	}

	protected ManagedChannelHandle getManagedChannelHandle() throws Exception {
		ManagedChannelHandle managedChannelHandle = this.pollConnection();
		if(null == managedChannelHandle){
			System.out.println(this.pool.getConfig().getConnectionTimeoutInMs());
			managedChannelHandle = (ManagedChannelHandle) this.freeConnections.poll(this.pool.getConfig().getConnectionTimeoutInMs(), TimeUnit.SECONDS);
		}
		System.out.println("获得连接---"+this.addr+":"+this.port+"--------池中还有连接："+this.getAvailableConnections()+"已经创建连接："+this.createdConnections);
	    return managedChannelHandle;
	}
    public void putConnectionBackInPartition(ManagedChannelHandle handle){
    	 try {
    		 if(this.createdConnections < this.maxConnections){
				if(!this.freeConnections.offer(handle)){
					 handle.shutdown();
					 handle = null;
				 }
    		 }
    		 System.out.println("回收连接---"+this.addr+":"+this.port+"--------池中还有连接："+this.getAvailableConnections()+"已经创建连接："+this.createdConnections);
		} catch (Exception e) {
			logger.debug(e.toString());
		}
    } 
	 public ManagedChannelHandle pollConnection(){
		 ManagedChannelHandle handle = null;
		 handle = this.freeConnections.poll();
		 if (!this.isUnableToCreateMoreTransactions()) { // unless we can't create any more connections...
		     this.maybeSignalForMoreConnections();  // see if we need to create more
		    }
		    return handle;
		  }
	
	public boolean checkChannel(ManagedChannelHandle managedChannelHandle) throws Exception {
		 ManagedChannel channel = managedChannelHandle.getManagedChannel();
		if ((channel != null) && (!channel.isShutdown()) && (!channel.isTerminated())) {
			return true;
		}
		return false;
	}

	protected void setFreeConnections(BlockingQueue<ManagedChannelHandle> freeConnections) throws Exception {
		this.freeConnections = freeConnections;
	}

	public ConnectionPartition(MChannelPool pool, int i) throws Exception{
			ManagedChannelConfig config = pool.getConfig();
			this.minConnections = config.getMinConnectionsPerPartition();
			this.maxConnections = config.getMaxConnectionsPerPartition();
			this.acquireIncrement = config.getIncrConnectNum();
			this.pool = pool;
			this.partitionAddrAndPort = config.getPartitionAddrAndPort()[i];
			String[] split = this.partitionAddrAndPort.split(":");
			this.addr = split[0];
			this.port = split[1];
	}

	public void fillConnections(int connectionsToCreate) {
		try {
			for (int i = 0; i < connectionsToCreate; i++) {
				this.addFreeConnection(new ManagedChannelHandle(this.addr, Integer.parseInt(this.port)));
			}
		} catch (Exception e) {
			logger.error("fillConnections fail: ", e);
		}
	}
	
	protected void maybeSignalForMoreConnections() {

		if (!this.isUnableToCreateMoreTransactions() && this.getAvailableConnections()*100/this.getMaxConnections() <= this.pool.getConfig().getPoolAvailabilityThreshold()){
			this.getPoolWatchThreadSignalQueue().offer(new Object()); // item being pushed is not important.
		}
	}
	public void shutdown(){
		for (int i = 0; i < this.freeConnections.size(); i++) {
			ManagedChannelHandle poll = this.freeConnections.poll();
			if(poll != null){
				try {
					poll.shutdown().awaitTermination(3L, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			else{
				break;
			}
		}
	}

	protected int getAcquireIncrement() {
		return this.acquireIncrement;
	}

	protected int getMinConnections() {
		return this.minConnections;
	}

	protected int getMaxConnections() {
		return this.maxConnections;
	}

	protected int getCreatedConnections() throws Exception {
		try {
			this.statsLock.readLock().lock();
			return this.createdConnections;
		} finally {
			this.statsLock.readLock().unlock();
		}
	}

	protected boolean isUnableToCreateMoreTransactions() {
		return this.unableToCreateMoreTransactions;
	}

	protected void setUnableToCreateMoreTransactions(boolean unableToCreateMoreTransactions) {
		this.unableToCreateMoreTransactions = unableToCreateMoreTransactions;
	}

	protected int getAvailableConnections() {
		return this.freeConnections.size();
	}

	public int getRemainingCapacity() {
		return this.freeConnections.remainingCapacity();
	}

	protected long getQueryExecuteTimeLimitinNanoSeconds() {
		return this.queryExecuteTimeLimitInNanoSeconds;
	}

	public boolean isDisableTracking() {
		return this.disableTracking;
	}

	public void setDisableTracking(boolean disableTracking) {
		this.disableTracking = disableTracking;
	}

	public long getQueryExecuteTimeLimitInNanoSeconds() {
		return this.queryExecuteTimeLimitInNanoSeconds;
	}

	public void setQueryExecuteTimeLimitInNanoSeconds(long queryExecuteTimeLimitInNanoSeconds) {
		this.queryExecuteTimeLimitInNanoSeconds = queryExecuteTimeLimitInNanoSeconds;
	}

	public String getPoolName() {
		return this.poolName;
	}

	public void setPoolName(String poolName) {
		this.poolName = poolName;
	}

	public MChannelPool getPool() {
		return this.pool;
	}

	public void setPool(MChannelPool pool) {
		this.pool = pool;
	}

	public String getAddr() {
		return this.addr;
	}

	public String getPort() {
		return this.port;
	}

	public void setCreatedConnections(int createdConnections) {
		this.createdConnections = createdConnections;
	}
	public String getPartitionAddrAndPort() {
		return partitionAddrAndPort;
	}
	public void setPartitionAddrAndPort(String partitionAddrAndPort) {
		this.partitionAddrAndPort = partitionAddrAndPort;
	}
	
}
