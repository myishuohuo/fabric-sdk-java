package org.hyperledger.fabric.sdk.ManageChannelPool;
import java.io.File;
import java.io.FileInputStream;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * @author oxchains.huohuo
 * */
public class ManagedChannelConfig implements Serializable, Cloneable {
	public void setCoreConnect(String coreConnect) {
		this.coreConnect = coreConnect;
	}
	private static final long serialVersionUID = 6090570773474131622L;
	private static final String DEFAULT_CONFIG = "channel.properties";
	public static final String ORG_HYPERLEDGER_FABRIC_SDK_CONFIGURATION = "org.hyperledger.fabric.sdk.configuration";
	private static Properties p = new Properties();
	/**
	 * Create more connections when we hit x% of our possible number of
	 * connections.
	 */
	private int poolAvailabilityThreshold = 0;
	
	/**
	 * After attempting to acquire a connection and failing, wait for this value
	 * before attempting to acquire a new connection again.
	 */
	private long acquireRetryDelayInMs = 7000L;
	/**
	 * A connection older than maxConnectionAge will be destroyed and purged
	 * from the pool.
	 */
	private long maxConnectionAgeInSeconds = 60*60*24L;
	/** Min number of connections per partition. */
	private int minConnectionsPerPartition = 50;
	/** Max number of connections per partition. */
	/** If set to true, the connection pool will remain empty until the first connection is obtained. */
	private boolean lazyInit = true;
	private int maxConnectionsPerPartition = 1000;
	/**
	 * Time to wait before a call to getConnection() times out and returns an
	 * error.
	 */
	private long connectionTimeoutInMs = 2L;
	/** Number of new connections to create in 1 batch. */
	private int incrConnectNum = 50;
	
	private String[] partitionAddrAndPort;
	
	private String coreConnect;
	/** Maximum age of an unused connection before it is closed off. */
	private long idleMaxAgeInSeconds = 3600L;
	/** Number of partitions. */
	private int partitionCount;
	/** Connections older than this are sent a keep-alive statement. */
	private long idleConnectionTestPeriodInSeconds = 14400L;
	private static final Log logger = LogFactory.getLog(ManagedChannelConfig.class);

	public ManagedChannelConfig() {
		File loadFile;
		FileInputStream configProps;
		try {
			loadFile = new File(System.getProperty(ORG_HYPERLEDGER_FABRIC_SDK_CONFIGURATION, DEFAULT_CONFIG))
					.getAbsoluteFile();
			logger.debug(String.format("Loading configuration from %s and it is present: %b", loadFile.toString(),
					loadFile.exists()));
			configProps = new FileInputStream(loadFile);
			p.load(configProps);
		} catch (Exception e) {
			logger.warn(
					String.format("Failed to load any configuration from: %s. Using toolkit defaults", DEFAULT_CONFIG));
		}
		loadProperties();
	}

	public void loadProperties() {
		try {
			if(null != p){
			setProperties(this.p);
			}
		}  catch (Exception e) {
			logger.error("ManagedChannelConfig loadProperties fail: ",e);
		}
	}

	private String lowerFirst(String name) {
		return name.substring(0, 1).toLowerCase() + name.substring(1);
	}

	public void setProperties(Properties props) {
		try {
			for (Method method : ManagedChannelConfig.class.getDeclaredMethods()) {
				String tmp = null;
				if (!method.getName().startsWith("set")) {
					continue;
				}
				tmp = lowerFirst(method.getName().substring(3));

				if ((method.getParameterTypes().length == 1) && (method.getParameterTypes()[0].equals(Integer.TYPE))) {
					String val = props.getProperty(tmp);
					if (val == null) {
						val = props.getProperty("channel.pool." + tmp);
					}
					if (val != null) {
						try {
							method.invoke(this, new Object[] { Integer.valueOf(Integer.parseInt(val)) });
						} catch (NumberFormatException localNumberFormatException) {
						}
					}
				} else if ((method.getParameterTypes().length == 1) && (method.getParameterTypes()[0].equals(Long.TYPE))) {
					String val = props.getProperty(tmp);
					if (val == null) {
						val = props.getProperty("channel.pool." + tmp);
					}
					if (val != null) {
						try {
							method.invoke(this, new Object[] { Long.valueOf(Long.parseLong(val)) });
						} catch (NumberFormatException localNumberFormatException1) {
						}
					}
				} else if ((method.getParameterTypes().length == 1)
						&& (method.getParameterTypes()[0].equals(String.class))) {
					String val = props.getProperty(tmp);
					if (val == null) {
						val = props.getProperty("channel.pool." + tmp);
					}
					if (val != null) {
						method.invoke(this, new Object[] { val });
					}
				}
				if ((method.getParameterTypes().length == 1) && (method.getParameterTypes()[0].equals(Boolean.TYPE))) {
					String val = props.getProperty(tmp);
					if (val == null) {
						val = props.getProperty("channel.pool." + tmp);
					}
					if (val != null) {
						method.invoke(this, new Object[] { Boolean.valueOf(Boolean.parseBoolean(val)) });
					}
				}
			}
			if (checkNullAndEmpty(this.coreConnect)) {
				this.partitionAddrAndPort = this.coreConnect.trim().split(",");
				this.partitionCount = this.partitionAddrAndPort.length;
			}
			else{
				this.partitionCount = 0;
			}
		} catch (Exception e) {
			logger.error("setProperties fail:",e);
		} 
	}
	public boolean checkNullAndEmpty(String name) {
		if (null == name || name.isEmpty()) {
			return false;
		}
		return true;
	}

	

	public ManagedChannelConfig clone() throws CloneNotSupportedException {
		ManagedChannelConfig clone = null;
		try {
			clone = (ManagedChannelConfig) super.clone();
			Field[] fields = getClass().getDeclaredFields();
			for (Field field : fields) {
		        field.set(clone, field.get(this));
			}
		} catch (Exception e) {
			logger.error("ManagedChannelConfig clone fail: ",e);
		}
		return clone;
	}

	public void sanitize() {
		try {
			if ((this.poolAvailabilityThreshold <= 0) || (this.poolAvailabilityThreshold > 100)) {
				this.poolAvailabilityThreshold = 20;
			}
			if (this.maxConnectionsPerPartition < 1 || this.maxConnectionsPerPartition > 1000) {
				this.maxConnectionsPerPartition = 1000;
			}
			if (this.minConnectionsPerPartition < 0) {
				this.minConnectionsPerPartition = 50;
			}
			if (this.minConnectionsPerPartition > this.maxConnectionsPerPartition) {
				this.minConnectionsPerPartition = this.maxConnectionsPerPartition;
			}
			if (this.incrConnectNum <= 0 || this.incrConnectNum > 300) {
				this.incrConnectNum = 300;
			}
			if (this.acquireRetryDelayInMs <= 0L) {
				this.acquireRetryDelayInMs = 1000L;
			}
		} catch (Exception e) {
			logger.error("ManagedChannelConfig sanitize fail: ",e);
		}
	}

	public long getIdleConnectionTestPeriod(TimeUnit timeUnit) {
		return timeUnit.convert(this.idleConnectionTestPeriodInSeconds, TimeUnit.SECONDS);
	}

	public long getIdleMaxAge(TimeUnit timeUnit) {
		return timeUnit.convert(this.idleMaxAgeInSeconds, TimeUnit.SECONDS);
	}

	public long getMaxConnectionAge() {
		return this.maxConnectionAgeInSeconds;
	}

	public long getMaxConnectionAgeInSeconds() {
		return this.maxConnectionAgeInSeconds;
	}

	public void setMaxConnectionAgeInSeconds(long maxConnectionAgeInSeconds) {
		this.maxConnectionAgeInSeconds = maxConnectionAgeInSeconds;
	}

	public long getMaxConnectionAge(TimeUnit timeUnit) {
		return timeUnit.convert(this.maxConnectionAgeInSeconds, TimeUnit.SECONDS);
	}

	public int getPoolAvailabilityThreshold() {
		return this.poolAvailabilityThreshold;
	}

	public void setPoolAvailabilityThreshold(int poolAvailabilityThreshold) {
		this.poolAvailabilityThreshold = poolAvailabilityThreshold;
	}

	public long getAcquireRetryDelayInMs() {
		return this.acquireRetryDelayInMs;
	}

	public void setAcquireRetryDelayInMs(long acquireRetryDelayInMs) {
		this.acquireRetryDelayInMs = acquireRetryDelayInMs;
	}

	public long getIdleConnectionTestPeriodInSeconds() {
		return this.idleConnectionTestPeriodInSeconds;
	}

	public void setIdleConnectionTestPeriodInSeconds(long idleConnectionTestPeriodInSeconds) {
		this.idleConnectionTestPeriodInSeconds = idleConnectionTestPeriodInSeconds;
	}
    
	public int getMinConnectionsPerPartition() {
		return this.minConnectionsPerPartition;
	}

	public void setMinConnectionsPerPartition(int minConnectionsPerPartition) {
		this.minConnectionsPerPartition = minConnectionsPerPartition;
	}

	public int getMaxConnectionsPerPartition() {
		return this.maxConnectionsPerPartition;
	}

	public void setMaxConnectionsPerPartition(int maxConnectionsPerPartition) {
		this.maxConnectionsPerPartition = maxConnectionsPerPartition;
	}

	public int getIncrConnectNum() {
		return this.incrConnectNum;
	}

	public void setIncrConnectNum(int incrConnectNum) {
		this.incrConnectNum = incrConnectNum;
	}

	public long getIdleMaxAgeInSeconds() {
		return this.idleMaxAgeInSeconds;
	}

	public void setIdleMaxAgeInSeconds(long idleMaxAgeInSeconds) {
		this.idleMaxAgeInSeconds = idleMaxAgeInSeconds;
	}

	public int getPartitionCount() {
		return this.partitionCount;
	}

	public String[] getPartitionAddrAndPort() {
		return partitionAddrAndPort;
	}

	
    public boolean isLazyInit(){
    	return lazyInit;
    }
    public void setLazyInit(boolean lazyInit) {
		this.lazyInit = lazyInit;
	}

	public long getConnectionTimeoutInMs() {
		return connectionTimeoutInMs;
	}

	public void setConnectionTimeoutInMs(long connectionTimeoutInMs) {
		this.connectionTimeoutInMs = connectionTimeoutInMs;
	}
}
