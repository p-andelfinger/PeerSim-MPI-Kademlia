package peersim.MPI.MessageHandler;

import peersim.MPI.Communicator;
import peersim.MPI.Logger;
import peersim.core.Network;
import peersim.core.Node;
import peersim.kademlia.KademliaProtocol;
import peersim.kademlia.RemoteLookup;
import peersim.kademlia.RemoteRankPeer;
import peersim.kademlia.events.FindNodeEvent;

public class FindNodeRequestHandler implements IMessageHandler {

	@Override
	public void handle(int[] message, int source) {
		FindNodeEvent evt = new FindNodeEvent(FindNodeEvent.EVT_RCV_FINDNODE_RQU);
		
		evt.src = Communicator.getRemoteRankPeer(new RemoteRankPeer(message[2], source));
		evt.target = message[4];
		
		Node n = Network.getById(message[3]);
		
		if(n == null) {
			Logger.log("BUG: don't know node with id " + message[3]);
			System.exit(1);
		}
		
		evt.operation = new RemoteLookup(message[5]);
		
		Communicator.addEventFromRemoteRank(message[1], evt, n, KademliaProtocol.kademliaid, source);
	}
	
	@Override
	public void handle(float[] message, int source) {
		
	}
}
