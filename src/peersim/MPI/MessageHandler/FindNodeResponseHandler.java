package peersim.MPI.MessageHandler;

import java.util.LinkedList;
import java.util.List;

import peersim.MPI.Communicator;
import peersim.MPI.Logger;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.IPeer;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.RemotePeer;
import peersim.kademlia.RemoteRankPeer;
import peersim.kademlia.events.FindNodeEvent;

public class FindNodeResponseHandler implements IMessageHandler {

	@Override
	public void handle(int[] message, int source) {
		List<IPeer> peers = new LinkedList<IPeer>();
		for(int i = 0; i < 8; i++) {
			int id = message[5 + i * 2];
			int rank = message[5 + i * 2 + 1];
			if(id == -1)
				continue;
			
			Node node = Network.getById(id);
			if(node != null) {
				peers.add((IPeer)node.getProtocol(KademliaProtocol.kademliaid));
			} else {
				peers.add(Communicator.getRemoteRankPeer(new RemoteRankPeer(id, rank)));
			}
		}
		
		FindNodeEvent evt = new FindNodeEvent(FindNodeEvent.EVT_RCV_FINDNODE_RSP);
		evt.body = peers;
		
		evt.src = Communicator.getRemoteRankPeer(new RemoteRankPeer(message[3], source));
		
		Node n = Network.getById(message[4]);
		
		if(n == null) {
			Logger.log("unknown node: " + message[4]);
			System.exit(1);
		}
		RemotePeer peer = (RemotePeer)n.getProtocol(KademliaProtocol.kademliaid);
		
		try {
			KademliaProtocol k = (KademliaProtocol)peer.getInnerPeer();
			evt.operation = k.getLookupById(message[1]);
			if(evt.operation == null) {
				Logger.log("unknown operation with id " + message[1]); // lookup seems to be done
				return;
			}
			
			Communicator.addEventFromRemoteRank(message[2], evt, n, KademliaProtocol.kademliaid, source);
		} catch(ClassCastException e) {
			Logger.log("cannot find node associated with incoming FindNodeResponse");
		}
	}
	
	@Override
	public void handle(float[] message, int source) {
		
	}
}
