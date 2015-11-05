package peersim.kademlia.events;

import peersim.kademlia.ILookup;
import peersim.kademlia.IPeer;

public class PingEvent extends KademliaEvent {
	public static final int EVT_PING = 10;
	public static final int EVT_PING_TMO = 11;
	public static final int EVT_PING_RSP = 12;
	
	public IPeer source;
	public int peerToAddId;
	public int lastReceived;
	
	
	public PingEvent(int type, IPeer source, int peerToAddId, int lastReceived) {
		super(type);
		this.source = source;
		this.peerToAddId = peerToAddId;
		this.lastReceived = lastReceived;
	}
	

}
