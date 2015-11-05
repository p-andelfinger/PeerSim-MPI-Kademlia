package peersim.kademlia;

import peersim.MPI.Logger;
import peersim.core.CommonState;
import peersim.core.Network;
import peersim.core.Node;
import peersim.core.Protocol;
import peersim.edsim.EDProtocol;
import peersim.edsim.EDSimulator;
import peersim.kademlia.events.PeerOfflineEvent;
import peersim.kademlia.events.PeerOnlineEvent;

public class PeerCreatorNode implements Node {
	public static final int EVENT_HANDLER_ID = 0;
	
	private int index;
	private CreationHandler handler;

	public PeerCreatorNode() {
		this.handler = new CreationHandler();
	}
	
	public PeerCreatorNode clone() {
		PeerCreatorNode result = null;
		try {
			result = (PeerCreatorNode) super.clone();
		} catch (CloneNotSupportedException e) {
		} // never happens
		CommonState.setNode(result);
		return result;
	}

	@Override
	public int getFailState() {
		return OK;
	}

	@Override
	public void setFailState(int failState) {
		// do nothing
	}

	@Override
	public boolean isUp() {
		return true;
	}

	@Override
	public Protocol getProtocol(int i) {
		return handler;
	}

	@Override
	public int protocolSize() {
		return 1;
	}

	@Override
	public void setIndex(int index) {
		this.index = index;
	}

	@Override
	public int getIndex() {
		return this.index;
	}

	@Override
	public long getID() {
		return 0;
	}

	private static class CreationHandler implements EDProtocol {

		@Override
		public Object clone() {
			return new CreationHandler();
		}
		
		@Override
		public void processEvent(Node notUsed, int pid, Object event) {
			if (event instanceof PeerOnlineEvent) {
				//Logger.log("creating a peer " + KademliaObserver.peersAdded);
				KademliaObserver.peersAdded++;

				PeerOnlineEvent evt = (PeerOnlineEvent) event;
				
				// create new peer and node
				Node node = Network.prototype.clone();
				Protocol prot = node.getProtocol(KademliaProtocol.kademliaid);
				if (!(prot instanceof RemotePeer)) 
					return;
				RemotePeer peer = (RemotePeer) prot;
				peer.init(node, Util.chooseRandomPeerId(), evt.getSessionLength() + CommonState.getTime());
				
				// set Location 
				peer.setLocation(evt.getLocation());
				
				// add node to network
				Network.add(node);
				
				//Network.addIPeer(peer);
				
				// generate peer offline event
				PeerOfflineEvent poff = new PeerOfflineEvent();
				EDSimulator.add(evt.getSessionLength(), poff, node, KademliaProtocol.kademliaid);
				
				// bootstrap peer
				peer.bootstrap();
				
			}
		}
		
	}
}
