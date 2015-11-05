package peersim.MPI.MessageHandler;

import peersim.MPI.Communicator;
import peersim.MPI.Logger;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.IPeer;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.Lookup;
import peersim.kademlia.RemotePeer;
import peersim.kademlia.RemoteRankPeer;
import peersim.kademlia.events.FindNodeEvent;

public class FindNodeTimeoutHandler implements IMessageHandler {

	@Override
	public void handle(int[] message, int source) {
		Node n = Network.getById(message[4]);
		
		if(n == null) {
			Logger.log("unknown node: " + message[4]);
			System.exit(1);
		}
		RemotePeer peer = (RemotePeer)n.getProtocol(KademliaProtocol.kademliaid);
		
		try {
			KademliaProtocol k = (KademliaProtocol)peer.getInnerPeer();
			Lookup op = k.getLookupById(message[1]);
			if(op == null) {
				Logger.log("unknown operation with id " + message[1]); // lookup seems to be done
				return;
			}
			
			IPeer src = Communicator.getRemoteRankPeer(new RemoteRankPeer(message[3], source));
			
			FindNodeEvent evt = FindNodeEvent.createFindNodeTimeoutEvent(op, src);
			Communicator.addEventFromRemoteRank(message[2], evt, n, KademliaProtocol.kademliaid, source);
			
		} catch(ClassCastException e) {
			Logger.log("cannot find node associated with incoming FindNodeRequestTimeout");
			// peer has gone down in the meantime, just drop message in this case
		}
	}
	
	@Override
	public void handle(float[] message, int source) {
		
	}
}
