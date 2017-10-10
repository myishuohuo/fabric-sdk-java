package org.hyperledger.fabric.sdk.ManageChannelPool;

import io.grpc.ManagedChannel;
/**
 * @author oxchains.huohuo
 * */
public class ManagedChannelPool {
	public static MChannelPool c = new MChannelPool();
	
	public static void releaseManagedChannel(ManagedChannelHandle m,String addr,String port){
		 try {
			c.releaseManagedChannelHandle(m,addr,port);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static ManagedChannelHandle getConnection(String addr,String port){
		ManagedChannelHandle ac = null;
		try {
			ac = c.getConnection(addr, port);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ac;
	}

}
