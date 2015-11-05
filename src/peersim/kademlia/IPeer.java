package peersim.kademlia;

import peersim.core.Node;
import peersim.kademlia.StateBuilder.Location;

public interface IPeer {

	public Node toNode();
	
	public int getId();
	
	public boolean canReceiveFrom(IPeer sender);
	
	public boolean canReceiveFrom(IPeer sender, long atTime);
	
	public boolean isOnline();

	public void processEvent(Node node, int pId, Object event);
	
	public void bootstrap();

	// public void onIncomingPing(IPeer from);
	
	public void setLocation(Location loc);

	public Location getLocation();

	//public void notifyAboutPingRequest(RemotePeer from);

}
