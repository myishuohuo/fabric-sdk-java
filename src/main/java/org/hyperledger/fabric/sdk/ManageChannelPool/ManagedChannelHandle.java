package org.hyperledger.fabric.sdk.ManageChannelPool;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bouncycastle.jcajce.provider.symmetric.Threefish;

import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
/**
 * @author oxchains.huohuo
 * */
public class ManagedChannelHandle {
	private static final Log logger = LogFactory.getLog(ManagedChannelHandle.class);
	/** the managedChannel */
	private ManagedChannel managedChannel = null;
	/** Last time this connection was used by an application. */
	private long connectionLastUsedInMs;
	/** Last time we sent a reset to this connection. */
	//private long connectionLastResetInMs;
	/** Time when this connection was created. */
	protected long connectionCreationTimeInMs;
    /** the channel addr*/
	private String addr = null;
	/** the channel port*/
    private Integer port = null;
	public ManagedChannelHandle(String addr ,int port) {
		try {
			this.managedChannel = NettyChannelBuilder.forAddress(addr,port).usePlaintext(true).build();
			this.connectionCreationTimeInMs = System.currentTimeMillis();
			this.addr = addr;
			this.port = port;
		} catch (Exception e) {
			logger.error("init ManagedChannelHandle fail: ",e);
		}
	}
	public void shutdownNow(){
		this.managedChannel.shutdownNow();
	}
	public ManagedChannel shutdown(){
		this.connectionLastUsedInMs = 0;
		this.connectionCreationTimeInMs =0;
		this.addr = null;
		this.port = null;
		return this.managedChannel.shutdown();
	}
	public boolean isShutdown(){
		return managedChannel.isShutdown();
	}
	public boolean isTerminated(){
		return managedChannel.isTerminated();
	}
	public void resetManagedChannel(){
		try {
			if(null != managedChannel && !managedChannel.isShutdown() && !managedChannel.isTerminated() ){
				managedChannel.shutdownNow();
			}
			managedChannel = NettyChannelBuilder.forAddress(addr,port).usePlaintext(true).build();
			this.connectionCreationTimeInMs = System.currentTimeMillis();
			this.connectionLastUsedInMs = 0;
		} catch (Exception e) {
			logger.debug(e.toString());
		}
	}
	
	public ManagedChannel getManagedChannel() {
		return managedChannel;
	}
	public void setManagedChannel(ManagedChannel managedChannel) {
		this.managedChannel = managedChannel;
	}
	public long getConnectionLastUsedInMs() {
		return connectionLastUsedInMs;
	}
	public void setConnectionLastUsedInMs(long connectionLastUsedInMs) {
		this.connectionLastUsedInMs = connectionLastUsedInMs;
	}
	public long getConnectionCreationTimeInMs() {
		return connectionCreationTimeInMs;
	}
	public void setConnectionCreationTimeInMs(long connectionCreationTimeInMs) {
		this.connectionCreationTimeInMs = connectionCreationTimeInMs;
	}
	public String getAddr() {
		return addr;
	}
	public void setAddr(String addr) {
		this.addr = addr;
	}

	public Integer getPort() {
		return port;
	}

	public void setPort(Integer port) {
		this.port = port;
	}
	
	

}
