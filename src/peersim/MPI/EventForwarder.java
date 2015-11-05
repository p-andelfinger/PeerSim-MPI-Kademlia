package peersim.MPI;

import java.util.List;

import peersim.core.CommonState;
import peersim.kademlia.IPeer;
import peersim.kademlia.RemoteRankPeer;
import peersim.kademlia.events.FindNodeEvent;
import peersim.kademlia.events.KademliaEvent;
import peersim.kademlia.events.PingEvent;

public class EventForwarder {

	private static int eventMessage[] = new int[6];
	private static int findNodeResponseMessage[] = new int[5 + 8 * 2];
	private static int pingMessage[] = new int[6];

	public static void forwardEvent(long latency, KademliaEvent event, RemoteRankPeer dest) {
		int targetRank = dest.getRank();
		
		if (event.getType() == FindNodeEvent.EVT_RCV_FINDNODE_RQU) {
			FindNodeEvent evt = (FindNodeEvent) event;
			eventMessage[0] = CommunicatorConfig.FIND_NODE_REQUEST_EVENT;
			eventMessage[1] = CommonState.getIntTime() + (int) latency;
			eventMessage[2] = evt.src.getId();
			eventMessage[3] = dest.getId();
			eventMessage[4] = evt.target;
			eventMessage[5] = evt.operation.getLookupId();
			Communicator.send(targetRank, eventMessage);
		} else if (event.getType() == FindNodeEvent.EVT_RCV_FINDNODE_RSP) {
			
			FindNodeEvent evt = (FindNodeEvent) event;
			int numPeers = ((List<IPeer>) evt.body).size();
			findNodeResponseMessage[0] = CommunicatorConfig.FIND_NODE_RESPONSE_EVENT;
			findNodeResponseMessage[1] = evt.operation.getLookupId();
			findNodeResponseMessage[2] = (int) CommonState.getTime()
					+ (int) latency;
			findNodeResponseMessage[3] = evt.src.getId();
			findNodeResponseMessage[4] = dest.getId();

			for (int i = 0; i < 8; i++) {
				findNodeResponseMessage[5 + i * 2] = findNodeResponseMessage[5 + i * 2 + 1] = -1;
			}
			for (int i = 0; i < numPeers; i++) {
				int id = ((List<IPeer>) evt.body).get(i).getId();
				findNodeResponseMessage[5 + i * 2] = id;
				RemoteRankPeer r = (RemoteRankPeer) Communicator.getRemoteRankPeer(id); 
				findNodeResponseMessage[5 + i * 2 + 1] = (r == null ? Communicator.getMyRank()
						: r.getRank());
			}

			Communicator.send(targetRank, findNodeResponseMessage);
		} else if (event.getType() == FindNodeEvent.EVT_RCV_FINDNODE_TMO) {
			FindNodeEvent evt = (FindNodeEvent) event;
			findNodeResponseMessage[0] = CommunicatorConfig.FIND_NODE_TIMEOUT_EVENT;
			findNodeResponseMessage[1] = evt.operation.getLookupId();
			findNodeResponseMessage[2] = (int) CommonState.getTime()
					+ (int) latency;
			findNodeResponseMessage[3] = evt.src.getId();
			findNodeResponseMessage[4] = dest.getId();
			Communicator.send(targetRank, findNodeResponseMessage);
		} else if (event.getType() == PingEvent.EVT_PING) {
			PingEvent evt = (PingEvent) event;
			pingMessage[0] = CommunicatorConfig.PING_REQUEST_EVENT;
			pingMessage[1] = CommonState.getIntTime() + (int) latency;
			pingMessage[2] = evt.source.getId();
			pingMessage[3] = dest.getId();
			pingMessage[4] = evt.peerToAddId; 
			pingMessage[5] = evt.lastReceived;
			Communicator.send(targetRank, pingMessage);
		} else if (event.getType() == PingEvent.EVT_PING_RSP) {
			PingEvent evt = (PingEvent) event;
			pingMessage[0] = CommunicatorConfig.PING_RESPONSE_EVENT;
			pingMessage[1] = CommonState.getIntTime() + (int) latency;
			pingMessage[2] = evt.source.getId();
			pingMessage[3] = dest.getId();
			pingMessage[4] = evt.peerToAddId;
			pingMessage[5] = evt.lastReceived;
			Communicator.send(targetRank, pingMessage);
		} else if (event.getType() == PingEvent.EVT_PING_TMO) {
			PingEvent evt = (PingEvent) event;
			pingMessage[0] = CommunicatorConfig.PING_TIMEOUT_EVENT;
			pingMessage[1] = CommonState.getIntTime() + (int) latency;
			pingMessage[2] = evt.source.getId();
			pingMessage[3] = dest.getId();
			pingMessage[4] = evt.peerToAddId;
			pingMessage[5] = evt.lastReceived;
			Communicator.send(targetRank, pingMessage);			
		}
	}
}
