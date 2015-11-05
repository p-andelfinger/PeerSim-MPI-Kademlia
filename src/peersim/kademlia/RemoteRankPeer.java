package peersim.kademlia;

import peersim.core.Node;
import peersim.kademlia.StateBuilder.Location;

public class RemoteRankPeer implements IPeer {

	private int nId;
	private final int rank;
	
	public int getRank() {
		return rank; // this could be replaced by some calculation based on the known id ranges per rank, should not take up that much memory though
	}
	
	public Node toNode() {
		return null;
	}

	public RemoteRankPeer(int nId, int rank) {
		this.nId = nId;
		
		this.rank = rank;
	}

	@Override
	public int getId() {
		return nId;
	}
	
	@Override
	public boolean canReceiveFrom(IPeer sender) {
		return true;
	}

	@Override
	public boolean canReceiveFrom(IPeer sender, long atTime) {
		return true;
	}

	@Override
	public boolean isOnline() {
		return true;
	}

	@Override
	public void processEvent(Node node, int pId, Object event) {
	}

	@Override
	public void bootstrap() {
		// TODO Auto-generated method stub

	}

	@Override
	public void setLocation(Location loc) {
		// TODO Auto-generated method stub

	}

	@Override
	public Location getLocation() {
		// TODO Auto-generated method stub
		return null;
	}

	/* @Override
	public void notifyAboutPingRequest(RemotePeer from) {
		// TODO Auto-generated method stub

	} */

}
